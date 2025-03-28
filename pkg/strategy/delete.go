package strategy

import (
	"context"

	"github.com/obot-platform/kinm/pkg/types"
	apierror "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/runtime/schema"
	genericapirequest "k8s.io/apiserver/pkg/endpoints/request"
	"k8s.io/apiserver/pkg/registry/rest"
	"k8s.io/apiserver/pkg/storage"
)

type ValidateDeleter interface {
	ValidateDelete(ctx context.Context, obj runtime.Object) *apierror.StatusError
}

type Deleter interface {
	Getter

	Delete(ctx context.Context, obj types.Object) (types.Object, error)
}

var _ rest.GracefulDeleter = (*DeleteAdapter)(nil)

func NewDelete(scheme *runtime.Scheme, strategy Deleter) *DeleteAdapter {
	return &DeleteAdapter{
		scheme:   scheme,
		strategy: strategy,
	}
}

type DeleteAdapter struct {
	scheme          *runtime.Scheme
	strategy        Deleter
	ValidateDeleter ValidateDeleter
}

func (a *DeleteAdapter) ObjectKinds(obj runtime.Object) ([]schema.GroupVersionKind, bool, error) {
	return a.scheme.ObjectKinds(obj)
}

func (a *DeleteAdapter) Recognizes(gvk schema.GroupVersionKind) bool {
	return a.scheme.Recognizes(gvk)
}

func (a *DeleteAdapter) Delete(ctx context.Context, name string, deleteValidation rest.ValidateObjectFunc, options *metav1.DeleteOptions) (runtime.Object, bool, error) {
	ctx, span := tracer.Start(ctx, "delete")
	defer span.End()

	ns, _ := genericapirequest.NamespaceFrom(ctx)
	obj, err := a.strategy.Get(ctx, ns, name)
	if err != nil {
		return nil, false, err
	}

	// support older consumers of delete by treating "nil" as delete immediately
	if options == nil {
		options = metav1.NewDeleteOptions(0)
	}
	var preconditions storage.Preconditions
	if options.Preconditions != nil {
		preconditions.UID = options.Preconditions.UID
		preconditions.ResourceVersion = options.Preconditions.ResourceVersion
		if err := preconditions.Check(name, obj); err != nil {
			return nil, false, err
		}
	}

	if deleteValidation != nil {
		err = deleteValidation(ctx, obj)
		if err != nil {
			return nil, false, err
		}
	}

	_, _, err = rest.BeforeDelete(a, ctx, obj, options)
	if err != nil {
		return nil, false, err
	}

	if !obj.GetDeletionTimestamp().IsZero() {
		return obj, false, nil
	}

	if a.ValidateDeleter != nil {
		if err := a.ValidateDeleter.ValidateDelete(ctx, obj); err != nil {
			return nil, false, err
		}
	}

	now := metav1.Now()
	obj.SetDeletionTimestamp(&now)

	if len(options.DryRun) != 0 && options.DryRun[0] == metav1.DryRunAll {
		return obj, false, nil
	}

	newObj, err := a.strategy.Delete(ctx, obj)
	return newObj, true, err
}
