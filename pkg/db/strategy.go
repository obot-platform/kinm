package db

import (
	"context"
	"database/sql"
	_ "embed"
	"encoding/json"
	"fmt"
	"iter"
	"math/rand/v2"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/obot-platform/kinm/pkg/db/statements"
	kotel "github.com/obot-platform/kinm/pkg/otel"
	"github.com/obot-platform/kinm/pkg/strategy"
	"github.com/obot-platform/kinm/pkg/types"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/trace"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/api/meta"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/fields"
	"k8s.io/apimachinery/pkg/labels"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/apimachinery/pkg/watch"
	"k8s.io/apiserver/pkg/storage"
	"k8s.io/klog/v2"
)

var (
	_ strategy.CompleteStrategy = (*Strategy)(nil)

	tracer = otel.Tracer("kinm/db")
)

type Strategy struct {
	db               db
	objTemplate      types.Object
	objListTemplate  types.ObjectList
	scheme           *runtime.Scheme
	cancelCompaction func()
	listener         *Listener
	unregister       func()

	broadcastLock sync.Mutex
	broadcast     chan struct{}
}

type record struct {
	id               int64
	name, namespace  string
	previousID       *int64
	uid              string
	vals             []any
	created, deleted int16
	value            string
}

func (r *record) Unmarshal(obj types.Object) error {
	if err := json.Unmarshal([]byte(r.value), obj); err != nil {
		return err
	}
	obj.SetResourceVersion(strconv.FormatInt(r.id, 10))
	return nil
}

// New builds the strategy for one table. A listener makes writes to that table
// announce themselves to other processes, and makes this process act on the writes
// those processes announce. Pass nil, as sqlite always does, to keep changes inside
// this process.
func New(ctx context.Context, sqlDB *sql.DB, gvk schema.GroupVersionKind, scheme *runtime.Scheme, tableName string, listener *Listener) (*Strategy, error) {
	objTemplate, err := scheme.New(gvk)
	if err != nil {
		return nil, err
	}
	objListTemplate, err := scheme.New(gvk.GroupVersion().WithKind(gvk.Kind + "List"))
	if err != nil {
		return nil, err
	}

	var fieldNames []string
	if o, ok := objTemplate.(types.Fields); ok {
		fieldNames = o.FieldNames()
	}

	var indexFields []string
	if o, ok := objTemplate.(types.FieldsIndexer); ok {
		indexFields = o.IndexFields()
	}

	newDB := db{
		sqlDB:  sqlDB,
		stmt:   statements.New(tableName, fieldNames, sqlDB.Stats().MaxOpenConnections != 1),
		gvk:    gvk,
		notify: listener != nil,
	}

	if err = newDB.migrate(ctx, fieldNames, indexFields); err != nil {
		return nil, err
	}

	s := &Strategy{
		db:              newDB,
		objTemplate:     objTemplate.(types.Object),
		objListTemplate: objListTemplate.(types.ObjectList),
		scheme:          scheme,
		listener:        listener,
		broadcast:       make(chan struct{}),
	}

	if listener != nil {
		// A write in another process ends up in the same place as a write in this
		// one, which is that every watch on this table wakes up and lists again.
		s.unregister = listener.Register(tableName, s.broadcastChange)
	}

	ctx, cancel := context.WithCancel(ctx)
	go func() {
		ticker := time.NewTicker(15 * time.Minute)
		defer ticker.Stop()
		for {
			select {
			case <-ctx.Done():
				return
			case <-ticker.C:
				if count, err := s.db.compact(ctx); err != nil {
					klog.Errorf("failed to compact %q: %v", tableName, err)
				} else if count > 0 {
					klog.Infof("compacted %q: %d records", tableName, count)
				}
			}
		}
	}()

	s.cancelCompaction = cancel
	return s, nil
}

func (s *Strategy) Create(ctx context.Context, object types.Object) (types.Object, error) {
	ctx, span := kotel.StartSpanIfParent(ctx, tracer, "dbStrategyCreate", trace.WithAttributes(kotel.ObjectToAttributes(object, attribute.String("gvk", s.db.gvk.String()))...))
	defer span.End()

	if object.GetUID() == "" {
		return nil, fmt.Errorf("object must have a UID")
	}

	defer s.broadcastChange()

	// On create all objects have a generation of 1
	object.SetGeneration(1)
	// All stored objects have a resource version of 0
	object.SetResourceVersion("0")

	var buf strings.Builder
	if err := json.NewEncoder(&buf).Encode(object); err != nil {
		return nil, err
	}

	var vals []any
	if o, ok := object.(types.Fields); ok {
		vals = make([]any, 0, len(o.FieldNames()))
		for _, f := range o.FieldNames() {
			vals = append(vals, o.Get(f))
		}
	}

	id, err := s.db.insert(ctx, record{
		name:      object.GetName(),
		namespace: object.GetNamespace(),
		uid:       string(object.GetUID()),
		created:   1,
		vals:      vals,
		value:     buf.String(),
	})
	if err != nil {
		return nil, err
	}

	result := object.DeepCopyObject().(types.Object)
	result.SetResourceVersion(strconv.FormatInt(id, 10))
	return result, nil
}

func (s *Strategy) New() types.Object {
	return s.objTemplate.DeepCopyObject().(types.Object)
}

func (s *Strategy) Get(ctx context.Context, namespace, name string) (types.Object, error) {
	attrs := []attribute.KeyValue{attribute.String("gvk", s.db.gvk.String())}
	if namespace != "" {
		attrs = append(attrs, attribute.String("namespace", namespace))
	}
	ctx, span := kotel.StartSpanIfParent(ctx, tracer, "dbStrategyGet", trace.WithAttributes(attrs...))
	defer span.End()

	rec, err := s.db.get(ctx, namespace, name)
	if err != nil {
		return nil, err
	}
	result := s.New()
	if err := json.Unmarshal([]byte(rec.value), result); err != nil {
		return nil, err
	}
	result.SetResourceVersion(strconv.FormatInt(rec.id, 10))
	return result, nil
}

func (s *Strategy) Update(ctx context.Context, obj types.Object) (types.Object, error) {
	ctx, span := kotel.StartSpanIfParent(ctx, tracer, "dbStrategyUpdate", trace.WithAttributes(kotel.ObjectToAttributes(obj, attribute.String("gvk", s.db.gvk.String()))...))
	defer span.End()

	defer s.broadcastChange()
	return s.doUpdate(ctx, obj, true)
}

func (s *Strategy) doUpdate(ctx context.Context, obj types.Object, updateGeneration bool) (types.Object, error) {
	var (
		buf             strings.Builder
		fieldNames      []string
		vals            []any
		resourceVersion int64
		err             error
	)

	if obj.GetResourceVersion() != "" {
		resourceVersion, err = strconv.ParseInt(obj.GetResourceVersion(), 10, 64)
		if err != nil {
			return nil, err
		}
	}

	obj = obj.DeepCopyObject().(types.Object)
	if updateGeneration {
		obj.SetGeneration(obj.GetGeneration() + 1)
	}
	// All stored objects have a resource version of 0
	obj.SetResourceVersion("0")

	if err = json.NewEncoder(&buf).Encode(obj); err != nil {
		return nil, err
	}

	if o, ok := obj.(types.Fields); ok {
		fieldNames = o.FieldNames()
		for _, f := range fieldNames {
			vals = append(vals, o.Get(f))
		}
	}

	rec := record{
		name:       obj.GetName(),
		namespace:  obj.GetNamespace(),
		previousID: &resourceVersion,
		uid:        string(obj.GetUID()),
		vals:       vals,
		value:      buf.String(),
	}

	var id int64
	if obj.GetDeletionTimestamp() != nil && len(obj.GetFinalizers()) == 0 {
		id, err = s.db.delete(ctx, rec)
	} else {
		id, err = s.db.insert(ctx, rec)
	}
	if err != nil {
		return nil, err
	}

	obj.SetResourceVersion(strconv.FormatInt(id, 10))
	return obj, nil
}

func (s *Strategy) UpdateStatus(ctx context.Context, obj types.Object) (types.Object, error) {
	ctx, span := kotel.StartSpanIfParent(ctx, tracer, "dbStrategyUpdateStatus", trace.WithAttributes(kotel.ObjectToAttributes(obj, attribute.String("gvk", s.db.gvk.String()))...))
	defer span.End()

	defer s.broadcastChange()
	return s.doUpdate(ctx, obj, false)
}

func (s *Strategy) prepareList(opts storage.ListOptions) (storage.ListOptions, error) {
	if opts.ResourceVersionMatch != "" {
		return opts, fmt.Errorf("resource version match is not supported")
	}

	if opts.Predicate.Label == nil {
		opts.Predicate.Label = labels.Everything()
	}
	if opts.Predicate.Field == nil {
		opts.Predicate.Field = fields.Everything()
	}
	if opts.Predicate.GetAttrs == nil {
		opts.Predicate.GetAttrs = storage.DefaultNamespaceScopedAttr
	}

	return opts, nil
}

func (s *Strategy) List(ctx context.Context, namespace string, opts storage.ListOptions) (types.ObjectList, error) {
	ctx, span := kotel.StartSpanIfParent(ctx, tracer, "dbStrategyList", trace.WithAttributes(kotel.ListOptionsToAttributes(opts, attribute.String("gvk", s.db.gvk.String()), attribute.String("namespace", namespace))...))
	defer span.End()

	var (
		objs       []runtime.Object
		listResult = s.NewList()
		err        error
	)

	opts, err = s.prepareList(opts)
	if err != nil {
		return nil, err
	}

	listResourceVersion, iter, err := newLister(ctx, &s.db, namespace, opts, false)
	if err != nil {
		return nil, err
	}

	for rec, err := range iter {
		if err != nil {
			return nil, err
		}

		obj := s.New()
		if err := rec.Unmarshal(obj); err != nil {
			return nil, err
		}

		if match, err := opts.Predicate.Matches(obj); err != nil {
			return nil, err
		} else if !match {
			continue
		}

		// We check this at the end because the next object could possibly not match the predicate so
		// we don't want to do continue token to them result in the next call being an empty list.
		if opts.Predicate.Limit > 0 && len(objs) >= int(opts.Predicate.Limit) {
			listResult.SetContinue(listResourceVersion + ":" + objs[len(objs)-1].(types.Object).GetResourceVersion())
			break
		}
		objs = append(objs, obj)
	}

	listResult.SetResourceVersion(listResourceVersion)
	return listResult, meta.SetList(listResult, objs)
}

func (s *Strategy) NewList() types.ObjectList {
	return s.objListTemplate.DeepCopyObject().(types.ObjectList)
}

func (s *Strategy) Delete(ctx context.Context, obj types.Object) (types.Object, error) {
	ctx, span := kotel.StartSpanIfParent(ctx, tracer, "dbStrategyDelete", trace.WithAttributes(kotel.ObjectToAttributes(obj, attribute.String("gvk", s.db.gvk.String()))...))
	defer span.End()

	defer s.broadcastChange()
	if obj.GetDeletionTimestamp() == nil {
		now := metav1.Now()
		obj.SetDeletionTimestamp(&now)
	}

	return s.doUpdate(ctx, obj, false)
}

func (s *Strategy) Watch(ctx context.Context, namespace string, opts storage.ListOptions) (<-chan watch.Event, error) {
	ctx, span := kotel.StartSpanIfParent(ctx, tracer, "dbStrategyWatch", trace.WithAttributes(kotel.ListOptionsToAttributes(opts, attribute.String("gvk", s.db.gvk.String()), attribute.String("namespace", namespace))...))
	defer span.End()

	opts, err := s.prepareList(opts)
	if err != nil {
		return nil, err
	}

	if opts.Predicate.Continue != "" {
		return nil, fmt.Errorf("continue is not supported in watch")
	}

	if opts.Predicate.Limit != 0 {
		return nil, fmt.Errorf("limit is not supported in watch")
	}

	if opts.ResourceVersion == "0" {
		opts.ResourceVersion = ""
	}

	// If resourceVersion is set we immediately go to watch phase and skip the historical list
	resourceVersion, lister, err := newLister(ctx, &s.db, namespace, opts, opts.ResourceVersion != "")
	if err != nil {
		return nil, err
	}

	opts.ResourceVersion = resourceVersion

	ch := make(chan watch.Event)
	go s.streamWatch(ctx, namespace, opts, lister, ch)
	return ch, nil
}

func toWatchEventError(err error) watch.Event {
	if _, ok := err.(apierrors.APIStatus); !ok {
		err = apierrors.NewInternalError(err)
	}
	status := err.(apierrors.APIStatus).Status()
	return watch.Event{
		Type:   watch.Error,
		Object: &status,
	}
}

func (s *Strategy) toWatchEvent(rec record) watch.Event {
	obj := s.New()
	if err := rec.Unmarshal(obj); err != nil {
		return toWatchEventError(err)
	}
	switch {
	case rec.created == 1:
		return watch.Event{Type: watch.Added, Object: obj}
	case rec.deleted == 1:
		return watch.Event{Type: watch.Deleted, Object: obj}
	default:
		return watch.Event{Type: watch.Modified, Object: obj}
	}
}

func (s *Strategy) broadcastChange() {
	s.broadcastLock.Lock()
	defer s.broadcastLock.Unlock()
	close(s.broadcast)
	s.broadcast = make(chan struct{})
}

func (s *Strategy) waitChange() <-chan struct{} {
	s.broadcastLock.Lock()
	defer s.broadcastLock.Unlock()
	return s.broadcast
}

func (s *Strategy) streamWatch(ctx context.Context, namespace string, opts storage.ListOptions, lister iter.Seq2[record, error], ch chan watch.Event) {
	defer close(ch)

	var bookmarks <-chan time.Time
	if opts.ProgressNotify {
		ticker := time.NewTicker(time.Minute)
		defer ticker.Stop()
		bookmarks = ticker.C
	}

	for {
		for rec, err := range lister {
			if err != nil {
				ch <- toWatchEventError(err)
				return
			}
			event := s.toWatchEvent(rec)
			if ok, err := opts.Predicate.Matches(event.Object); err != nil {
				ch <- toWatchEventError(err)
			} else if ok {
				ch <- event
			}
		}

		var (
			newResourceVersion string
			err                error
		)

		// Take the channel before listing rather than after. A write that commits
		// between the list and the wait has already closed this channel, so the
		// wait returns immediately instead of running for a whole poll interval
		// over a change that has already happened.
		changed := s.waitChange()

		newResourceVersion, lister, err = newLister(ctx, &s.db, namespace, opts, true)
		if err != nil {
			ch <- toWatchEventError(err)
			return
		}

		if newResourceVersion == opts.ResourceVersion {
			select {
			case <-ctx.Done():
				return
			case <-bookmarks:
				ch <- watch.Event{Type: watch.Bookmark, Object: nil}
			case <-changed:
			case <-time.After(s.watchPollDelay()):
			}
		}

		opts.ResourceVersion = newResourceVersion
	}
}

// watchPollDelay is how long a watch waits for something to happen before it lists
// anyway.
//
// broadcastChange covers writes made in this process, and a connected listener
// covers writes made in every other process, so with both in place the poll only
// matters when a notification is missed and it can be long. Without a listener the
// poll is the only way to see another process's write, so it stays at the two
// seconds kinm has always used.
func (s *Strategy) watchPollDelay() time.Duration {
	if s.listener == nil || !s.listener.Connected() {
		return fallbackWatchPollInterval
	}
	// Every table in a process starts watching at about the same moment, and
	// processes restart together during a rollout. The jitter spreads the polls out
	// so that they do not all run at the same time.
	return watchPollInterval + rand.N(watchPollInterval/4)
}

func (s *Strategy) Destroy() {
	if s.unregister != nil {
		s.unregister()
	}
	s.cancelCompaction()
	s.db.Close()
}

func (s *Strategy) Scheme() *runtime.Scheme {
	return s.scheme
}
