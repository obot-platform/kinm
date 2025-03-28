package errors

import (
	"errors"
	"fmt"

	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/apiserver/pkg/storage"
)

const (
	OptimisticLockErrorMsg = "the object has been modified; please apply your changes to the latest version and try again"
)

func NewConflict(gvk schema.GroupVersionKind, name string, err error) error {
	return apierrors.NewConflict(
		schema.GroupResource{
			Group:    gvk.Group,
			Resource: gvk.Kind,
		}, name, err)
}

func NewAlreadyExists(gvk schema.GroupVersionKind, name string) error {
	return apierrors.NewAlreadyExists(
		schema.GroupResource{
			Group:    gvk.Group,
			Resource: gvk.Kind,
		}, name)
}

func NewNotFound(gvk schema.GroupVersionKind, name string) error {
	return apierrors.NewNotFound(
		schema.GroupResource{
			Group:    gvk.Group,
			Resource: gvk.Kind,
		}, name)
}

func NewCompactionError(requested, current uint) error {
	return apierrors.NewResourceExpired(fmt.Sprintf("resource version %d before current compaction %d", requested, current))
}

func NewUIDMismatch(name, oldUID, newUID string) error {
	err := fmt.Sprintf(
		"Precondition failed: UID in precondition: %v, UID in object meta: %v", oldUID, newUID)
	return storage.NewInvalidObjError(name, err)
}

func NewResourceVersionMismatch(gvk schema.GroupVersionKind, name string) error {
	return apierrors.NewConflict(schema.GroupResource{
		Group:    gvk.Group,
		Resource: gvk.Kind,
	}, name, errors.New(OptimisticLockErrorMsg))
}
