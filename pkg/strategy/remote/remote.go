package remote

import (
	"context"

	kotel "github.com/obot-platform/kinm/pkg/otel"
	"github.com/obot-platform/kinm/pkg/strategy"
	"github.com/obot-platform/kinm/pkg/types"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/trace"
	"k8s.io/apimachinery/pkg/fields"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/apimachinery/pkg/watch"
	"k8s.io/apiserver/pkg/storage"
	kclient "sigs.k8s.io/controller-runtime/pkg/client"
)

var (
	_ strategy.CompleteStrategy = (*Remote)(nil)

	tracer = otel.Tracer("kinm/remote")
)

type Remote struct {
	gvk     schema.GroupVersionKind
	obj     types.Object
	objList types.ObjectList
	c       kclient.WithWatch
}

func NewRemote(obj types.Object, c kclient.WithWatch) (*Remote, error) {
	gvk, err := c.GroupVersionKindFor(obj)
	if err != nil {
		return nil, err
	}
	return &Remote{
		gvk:     gvk,
		obj:     obj,
		objList: types.MustGetListType(obj, c.Scheme()),
		c:       c,
	}, nil
}

func (r *Remote) Create(ctx context.Context, object types.Object) (types.Object, error) {
	ctx, span := tracer.Start(ctx, "create", trace.WithAttributes(kotel.ObjectToAttributes(object, attribute.String("gvk", r.gvk.String()))...))
	defer span.End()

	return object, r.c.Create(ctx, object)
}

func (r *Remote) New() types.Object {
	return r.obj.DeepCopyObject().(types.Object)
}

func (r *Remote) Get(ctx context.Context, namespace, name string) (types.Object, error) {
	obj := r.New()
	return obj, r.c.Get(ctx, kclient.ObjectKey{Namespace: namespace, Name: name}, obj)
}

func (r *Remote) Update(ctx context.Context, obj types.Object) (types.Object, error) {
	ctx, span := tracer.Start(ctx, "update", trace.WithAttributes(kotel.ObjectToAttributes(obj, attribute.String("gvk", r.gvk.String()))...))
	defer span.End()

	return obj, r.c.Update(ctx, obj)
}

func (r *Remote) UpdateStatus(ctx context.Context, obj types.Object) (types.Object, error) {
	ctx, span := tracer.Start(ctx, "updateStatus", trace.WithAttributes(kotel.ObjectToAttributes(obj, attribute.String("gvk", r.gvk.String()))...))
	defer span.End()

	return obj, r.c.Status().Update(ctx, obj)
}

func (r *Remote) GetToList(ctx context.Context, namespace, name string) (types.ObjectList, error) {
	ctx, span := tracer.Start(ctx, "getToList", trace.WithAttributes(kotel.ObjectToAttributes(r.obj, attribute.String("gvk", r.gvk.String()))...))
	defer span.End()

	list := r.NewList()
	return list, r.c.List(ctx, list, &kclient.ListOptions{
		FieldSelector: fields.SelectorFromSet(map[string]string{
			"metadata.name":      name,
			"metadata.namespace": namespace,
		}),
		Limit: 1,
	})
}

func (r *Remote) List(ctx context.Context, namespace string, opts storage.ListOptions) (types.ObjectList, error) {
	ctx, span := tracer.Start(ctx, "list", trace.WithAttributes(kotel.ListOptionsToAttributes(opts, attribute.String("gvk", r.gvk.String()), attribute.String("namespace", namespace))...))
	defer span.End()

	list := r.NewList()
	return list, r.c.List(ctx, list, strategy.ToListOpts(namespace, opts))
}

func (r *Remote) NewList() types.ObjectList {
	return r.objList.DeepCopyObject().(types.ObjectList)
}

func (r *Remote) Delete(ctx context.Context, obj types.Object) (types.Object, error) {
	ctx, span := tracer.Start(ctx, "delete", trace.WithAttributes(kotel.ObjectToAttributes(obj, attribute.String("gvk", r.gvk.String()))...))
	defer span.End()

	return obj, r.c.Delete(ctx, obj)
}

func (r *Remote) Watch(ctx context.Context, namespace string, opts storage.ListOptions) (<-chan watch.Event, error) {
	ctx, span := tracer.Start(ctx, "watch", trace.WithAttributes(kotel.ListOptionsToAttributes(opts, attribute.String("gvk", r.gvk.String()), attribute.String("namespace", namespace))...))
	defer span.End()

	list := r.NewList()
	listOpts := strategy.ToListOpts(namespace, opts)
	w, err := r.c.Watch(ctx, list, listOpts)
	if err != nil {
		return nil, err
	}
	return w.ResultChan(), nil
}

func (r *Remote) Destroy() {
}

func (r *Remote) Scheme() *runtime.Scheme {
	return r.c.Scheme()
}
