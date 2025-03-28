package strategy

import (
	"context"
	"os"
	"strconv"

	"github.com/obot-platform/kinm/pkg/types"
	"github.com/obot-platform/kinm/pkg/validator"
	"k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/api/meta"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/util/validation/field"
	"k8s.io/apiserver/pkg/endpoints/request"
	"k8s.io/apiserver/pkg/registry/rest"
	"k8s.io/apiserver/pkg/storage/names"
)

type CompleteCRUD interface {
	Lister
	Watcher
	Creater
	Updater
	Deleter
}

type Creater interface {
	Create(ctx context.Context, object types.Object) (types.Object, error)
	New() types.Object
}

type WarningsOnCreator interface {
	WarningsOnCreate(ctx context.Context, obj runtime.Object) []string
}

type NameValidator interface {
	ValidateName(ctx context.Context, obj runtime.Object) field.ErrorList
}

type Validator interface {
	Validate(ctx context.Context, obj runtime.Object) field.ErrorList
}

type PrepareForCreator interface {
	PrepareForCreate(ctx context.Context, obj runtime.Object)
}

var _ rest.Creater = (*CreateAdapter)(nil)

func NewCreate(schema *runtime.Scheme, strategy Creater) *CreateAdapter {
	generateNameCollisionRetryLimit := 5
	if newLimit := os.Getenv("KINM_GENERATE_NAME_COLLISION_RETRY_LIMIT"); newLimit != "" {
		if limit, err := strconv.Atoi(newLimit); err == nil {
			generateNameCollisionRetryLimit = limit
		}
	}
	return &CreateAdapter{
		NameGenerator:          names.SimpleNameGenerator,
		Scheme:                 schema,
		strategy:               strategy,
		generateNameRetryLimit: generateNameCollisionRetryLimit,
	}
}

type CreateAdapter struct {
	names.NameGenerator
	*runtime.Scheme
	strategy               Creater
	Warner                 WarningsOnCreator
	Validator              Validator
	NameValidator          NameValidator
	PrepareForCreater      PrepareForCreator
	generateNameRetryLimit int
}

func (a *CreateAdapter) New() runtime.Object {
	return a.strategy.New()
}

func (a *CreateAdapter) Create(ctx context.Context, obj runtime.Object, createValidation rest.ValidateObjectFunc, options *metav1.CreateOptions) (runtime.Object, error) {
	ctx, span := tracer.Start(ctx, "create")
	defer span.End()

	retriesRenaming := a.generateNameRetryLimit
	original := obj.DeepCopyObject()

	for {
		if objectMeta, err := meta.Accessor(obj); err == nil {
			rest.FillObjectMetaSystemFields(objectMeta)
			if objectMeta.GetName() == "" {
				requestInfo, ok := request.RequestInfoFrom(ctx)
				if ok && requestInfo.Name != "" {
					objectMeta.SetName(requestInfo.Name)
				}
			}

			if len(objectMeta.GetGenerateName()) > 0 && len(objectMeta.GetName()) == 0 {
				objectMeta.SetName(a.GenerateName(objectMeta.GetGenerateName()))
			} else {
				// Don't retry on already exists errors
				retriesRenaming = 0
			}
		} else {
			return nil, err
		}

		if err := rest.BeforeCreate(a, ctx, obj); err != nil {
			return nil, err
		}

		// at this point we have a fully formed object.  It is time to call the validators that the apiserver
		// handling chain wants to enforce.
		if createValidation != nil {
			if err := createValidation(ctx, obj.DeepCopyObject()); err != nil {
				return nil, err
			}
		}

		if len(options.DryRun) != 0 && options.DryRun[0] == metav1.DryRunAll {
			return obj, nil
		}

		newObj, err := a.strategy.Create(ctx, obj.(types.Object))
		if retriesRenaming <= 1 || !errors.IsAlreadyExists(err) {
			// Retry at most 5 times.
			return newObj, err
		}

		retriesRenaming--
		obj = original.DeepCopyObject()
	}
}

func (a *CreateAdapter) PrepareForCreate(ctx context.Context, obj runtime.Object) {
	if a.PrepareForCreater != nil {
		a.PrepareForCreater.PrepareForCreate(ctx, obj)
	} else if o, ok := a.strategy.(PrepareForCreator); ok {
		o.PrepareForCreate(ctx, obj)
	}
}

func checkNamespace(nsed bool, obj runtime.Object) *field.Error {
	o := obj.(types.Object)
	if nsed && o.GetNamespace() == "" {
		return field.Forbidden(field.NewPath("metadata", "namespace"), "namespace must be set for namespaced scoped resource")
	} else if !nsed && o.GetNamespace() != "" {
		return field.Forbidden(field.NewPath("metadata", "namespace"), "namespace must not be set for cluster scoped resource")
	}
	return nil
}

func (a *CreateAdapter) Validate(ctx context.Context, obj runtime.Object) (result field.ErrorList) {
	if a.NameValidator != nil {
		result = append(result, a.NameValidator.ValidateName(ctx, obj)...)
	} else if o, ok := a.strategy.(NameValidator); ok {
		result = append(result, o.ValidateName(ctx, obj)...)
	} else {
		result = append(result, validator.ValidDNSSubdomain.ValidateName(ctx, obj)...)
	}
	if err := checkNamespace(a.NamespaceScoped(), obj); err != nil {
		result = append(result, err)
	}
	if a.Validator != nil {
		result = append(result, a.Validator.Validate(ctx, obj)...)
	} else if o, ok := a.strategy.(Validator); ok {
		result = append(result, o.Validate(ctx, obj)...)
	}
	return
}

func (a *CreateAdapter) WarningsOnCreate(ctx context.Context, obj runtime.Object) []string {
	if a.Warner != nil {
		return a.Warner.WarningsOnCreate(ctx, obj)
	}
	if o, ok := a.strategy.(WarningsOnCreator); ok {
		return o.WarningsOnCreate(ctx, obj)
	}
	return nil
}

func (a *CreateAdapter) Canonicalize(obj runtime.Object) {
}

func (a *CreateAdapter) NamespaceScoped() bool {
	if o, ok := a.strategy.(types.NamespaceScoper); ok {
		return o.NamespaceScoped()
	}
	if o, ok := a.strategy.New().(types.NamespaceScoper); ok {
		return o.NamespaceScoped()
	}
	return true
}
