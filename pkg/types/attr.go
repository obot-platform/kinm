package types

import (
	"k8s.io/apimachinery/pkg/fields"
	"k8s.io/apimachinery/pkg/labels"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apiserver/pkg/storage"
)

func DefaultGetAttr(scoper NamespaceScoper) storage.AttrFunc {
	return func(obj runtime.Object) (labels.Set, fields.Set, error) {
		ls, fs := labels.Set{}, fields.Set{}

		var baseFunc storage.AttrFunc = storage.DefaultNamespaceScopedAttr
		if !scoper.NamespaceScoped() {
			baseFunc = storage.DefaultClusterScopedAttr
		}

		l, f, err := baseFunc(obj)
		if err != nil {
			return nil, nil, err
		}
		for k, v := range l {
			ls[k] = v
		}
		for k, v := range f {
			fs[k] = v
		}

		if f, ok := obj.(Fields); ok {
			for _, field := range f.FieldNames() {
				fs[field] = f.Get(field)
			}
		}

		return ls, fs, nil
	}
}
