package db

import (
	"context"
	"strconv"
	"testing"

	"github.com/obot-platform/kinm/pkg/types"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/fields"
	"k8s.io/apimachinery/pkg/labels"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/runtime/schema"
	ktypes "k8s.io/apimachinery/pkg/types"
	"k8s.io/apimachinery/pkg/watch"
	"k8s.io/apiserver/pkg/storage"
	kclient "sigs.k8s.io/controller-runtime/pkg/client"
)

var ctx = context.Background()

var testGVK = schema.GroupVersionKind{
	Group:   "testgroup",
	Version: "testversion",
	Kind:    "TestKind",
}

type TestKind struct {
	metav1.TypeMeta   `json:",inline"`
	metav1.ObjectMeta `json:"metadata,omitempty"`
	Spec              struct {
		NewValue   string `json:"newValue,omitempty"`
		OtherValue string `json:"otherValue,omitempty"`
	} `json:"spec,omitempty"`
	Value string `json:"value,omitempty"`
}

func (t *TestKind) DeepCopyObject() runtime.Object {
	return &TestKind{
		TypeMeta:   t.TypeMeta,
		ObjectMeta: *t.ObjectMeta.DeepCopy(),
		Value:      t.Value,
	}
}

func (t *TestKind) FieldNames() []string {
	return []string{"spec.newValue", "spec.otherValue"}
}

func (t *TestKind) Get(k string) string {
	if t != nil {
		switch k {
		case "spec.newValue":
			return t.Spec.NewValue
		case "spec.otherValue":
			return t.Spec.OtherValue
		}
	}
	return ""
}

func (t *TestKind) Has(k string) bool {
	return t.Get(k) != ""
}

func (*TestKind) IndexFields() []string {
	return []string{"spec.newValue", "spec.otherValue"}
}

func (*TestKind) NamespaceScoped() bool {
	return true
}

// TestKindList contains a list of TestKind
type TestKindList struct {
	metav1.TypeMeta `json:",inline"`
	metav1.ListMeta `json:"metadata,omitempty"`
	Items           []TestKind `json:"items"`
}

func (t *TestKindList) DeepCopyObject() runtime.Object {
	// This is obviously wrong, but it's just a test
	return &TestKindList{}
}

func newStrategy(t *testing.T) *Strategy {
	t.Helper()

	schema := runtime.NewScheme()
	schema.AddKnownTypes(testGVK.GroupVersion(), &TestKind{}, &TestKindList{})

	db := newDatabase(t)
	_, err := db.sqlDB.Exec("DROP TABLE IF EXISTS strategytest")
	require.NoError(t, err)
	s, err := New(ctx, db.sqlDB, testGVK, schema, "strategytest")
	require.NoError(t, err)

	for i := range 3 {
		suffix := strconv.Itoa(i + 1)
		_, err = s.Create(context.Background(), &TestKind{
			ObjectMeta: metav1.ObjectMeta{
				Name:      "testname" + suffix,
				Namespace: "testnamespace" + suffix,
				UID:       ktypes.UID("testuid" + suffix),
				Labels: map[string]string{
					"test": suffix,
				},
			},
			Value: "testvalue" + suffix,
			Spec: struct {
				NewValue   string `json:"newValue,omitempty"`
				OtherValue string `json:"otherValue,omitempty"`
			}{
				NewValue:   "newvalue" + suffix,
				OtherValue: "othervalue" + suffix,
			},
		})
		require.NoError(t, err)
	}

	return s
}

func TestStrategyListDefault(t *testing.T) {
	s := newStrategy(t)
	result, err := s.List(context.Background(), "", storage.ListOptions{})
	require.NoError(t, err)

	list := result.(*TestKindList)

	require.Len(t, list.Items, 3)

	assert.Equal(t, "testname1", list.Items[0].Name)
	assert.Equal(t, "1", list.Items[0].ResourceVersion)
	assert.Equal(t, "testname2", list.Items[1].Name)
	assert.Equal(t, "2", list.Items[1].ResourceVersion)
	assert.Equal(t, "testname3", list.Items[2].Name)
	assert.Equal(t, "3", list.Items[2].ResourceVersion)
}

func TestStrategyListFieldSelector(t *testing.T) {
	s := newStrategy(t)
	result, err := s.List(context.Background(), "", storage.ListOptions{
		Predicate: storage.SelectionPredicate{
			Field: fields.SelectorFromSet(map[string]string{
				"spec.newValue": "newvalue2",
			}),
			GetAttrs: types.DefaultGetAttr(new(TestKind)),
		},
	})
	require.NoError(t, err)

	list := result.(*TestKindList)

	require.Len(t, list.Items, 1)

	assert.Equal(t, "testname2", list.Items[0].Name)
	assert.Equal(t, "newvalue2", list.Items[0].Spec.NewValue)
	assert.Equal(t, "othervalue2", list.Items[0].Spec.OtherValue)

	result, err = s.List(context.Background(), "", storage.ListOptions{
		Predicate: storage.SelectionPredicate{
			Field: fields.SelectorFromSet(map[string]string{
				"spec.newValue":   "newvalue2",
				"spec.otherValue": "othervalue2",
			}),
			GetAttrs: types.DefaultGetAttr(new(TestKind)),
		},
	})
	require.NoError(t, err)

	list = result.(*TestKindList)

	require.Len(t, list.Items, 1)

	assert.Equal(t, "testname2", list.Items[0].Name)
	assert.Equal(t, "newvalue2", list.Items[0].Spec.NewValue)
	assert.Equal(t, "othervalue2", list.Items[0].Spec.OtherValue)

	result, err = s.List(context.Background(), "", storage.ListOptions{
		Predicate: storage.SelectionPredicate{
			Field: fields.SelectorFromSet(map[string]string{
				"spec.newValue":   "newvalue1",
				"spec.otherValue": "othervalue2",
			}),
			GetAttrs: types.DefaultGetAttr(new(TestKind)),
		},
	})
	require.NoError(t, err)

	list = result.(*TestKindList)

	require.Len(t, list.Items, 0)
}

func TestStrategyListRV(t *testing.T) {
	s := newStrategy(t)
	result, err := s.List(context.Background(), "", storage.ListOptions{
		ResourceVersion: "2",
	})
	require.NoError(t, err)

	list := result.(*TestKindList)

	require.Len(t, list.Items, 2)

	assert.Equal(t, "testname1", list.Items[0].Name)
	assert.Equal(t, "1", list.Items[0].ResourceVersion)
	assert.Equal(t, "testname2", list.Items[1].Name)
	assert.Equal(t, "2", list.Items[1].ResourceVersion)
}

func TestStrategyListFilterNeedTwoChunks(t *testing.T) {
	s := newStrategy(t)
	result, err := s.List(context.Background(), "", storage.ListOptions{
		Predicate: storage.SelectionPredicate{
			Limit: 1,
			Label: labels.SelectorFromSet(map[string]string{
				"test": "3",
			}),
		},
	})
	require.NoError(t, err)

	list := result.(*TestKindList)

	require.Len(t, list.Items, 1)

	assert.Equal(t, "3", list.ResourceVersion)
	assert.Equal(t, "", list.Continue)
	assert.Equal(t, "testname3", list.Items[0].Name)
}

func TestStrategyDeleteNeedRevision(t *testing.T) {
	s := newStrategy(t)
	_, err := s.Delete(context.Background(), &TestKind{
		ObjectMeta: metav1.ObjectMeta{
			Name: "testname1",
		},
	})
	assert.True(t, apierrors.IsConflict(err))
}

func TestStrategyDelete(t *testing.T) {
	s := newStrategy(t)
	result, err := s.Get(ctx, "", "testname3")
	require.NoError(t, err)
	assert.Equal(t, "3", result.GetResourceVersion())
	assert.Equal(t, "testname3", result.GetName())

	result, err = s.Delete(context.Background(), result)
	require.NoError(t, err)
	assert.Equal(t, "4", result.GetResourceVersion())
}

func TestWatchNoRv(t *testing.T) {
	ctx, cancel := context.WithCancel(ctx)
	defer cancel()

	s := newStrategy(t)
	w, err := s.Watch(ctx, "", storage.ListOptions{})
	require.NoError(t, err)

	event := <-w
	assert.Equal(t, watch.Added, event.Type)
	assert.Equal(t, "1", event.Object.(kclient.Object).GetResourceVersion())
	assert.Equal(t, "testname1", event.Object.(kclient.Object).GetName())

	event = <-w
	assert.Equal(t, watch.Added, event.Type)
	assert.Equal(t, "2", event.Object.(kclient.Object).GetResourceVersion())
	assert.Equal(t, "testname2", event.Object.(kclient.Object).GetName())

	event = <-w
	assert.Equal(t, watch.Added, event.Type)
	assert.Equal(t, "3", event.Object.(kclient.Object).GetResourceVersion())
	assert.Equal(t, "testname3", event.Object.(kclient.Object).GetName())
}

func TestWatchRv(t *testing.T) {
	ctx, cancel := context.WithCancel(ctx)
	defer cancel()

	s := newStrategy(t)
	w, err := s.Watch(ctx, "", storage.ListOptions{
		ResourceVersion: "2",
	})
	require.NoError(t, err)

	event := <-w
	assert.Equal(t, watch.Added, event.Type)
	assert.Equal(t, "3", event.Object.(kclient.Object).GetResourceVersion())
	assert.Equal(t, "testname3", event.Object.(kclient.Object).GetName())
}

func TestWatchChanges(t *testing.T) {
	ctx, cancel := context.WithCancel(ctx)
	defer cancel()

	s := newStrategy(t)
	w, err := s.Watch(ctx, "", storage.ListOptions{
		ResourceVersion: "2",
	})
	require.NoError(t, err)

	event := <-w
	assert.Equal(t, watch.Added, event.Type)
	assert.Equal(t, "3", event.Object.(kclient.Object).GetResourceVersion())
	assert.Equal(t, "testname3", event.Object.(kclient.Object).GetName())

	test1, err := s.Get(ctx, "", "testname1")
	require.NoError(t, err)

	test2, err := s.Get(ctx, "", "testname2")
	require.NoError(t, err)

	_, err = s.Delete(ctx, test1)
	require.NoError(t, err)

	test2.(*TestKind).Value = "newvalue"
	_, err = s.Update(ctx, test2)
	require.NoError(t, err)

	event = <-w
	assert.Equal(t, watch.Deleted, event.Type)
	assert.Equal(t, "4", event.Object.(kclient.Object).GetResourceVersion())
	assert.Equal(t, "testname1", event.Object.(kclient.Object).GetName())

	event = <-w
	assert.Equal(t, watch.Modified, event.Type)
	assert.Equal(t, "5", event.Object.(kclient.Object).GetResourceVersion())
	assert.Equal(t, "testname2", event.Object.(kclient.Object).GetName())
}

func TestContinue(t *testing.T) {
	s := newStrategy(t)
	_, err := s.Delete(context.Background(), &TestKind{
		ObjectMeta: metav1.ObjectMeta{
			Name:            "testname1",
			UID:             "testuid1",
			ResourceVersion: "1",
		},
	})
	require.NoError(t, err)

	_, err = s.Delete(context.Background(), &TestKind{
		ObjectMeta: metav1.ObjectMeta{
			Name:            "testname2",
			UID:             "testuid2",
			ResourceVersion: "2",
		},
	})
	require.NoError(t, err)

	_, err = s.Delete(context.Background(), &TestKind{
		ObjectMeta: metav1.ObjectMeta{
			Name:            "testname3",
			UID:             "testuid3",
			ResourceVersion: "3",
		},
	})
	require.NoError(t, err)

	res, err := s.List(context.Background(), "", storage.ListOptions{
		Predicate: storage.SelectionPredicate{
			Limit: 1,
		},
		ResourceVersion: "3",
	})
	require.NoError(t, err)

	list := res.(*TestKindList)
	require.Len(t, list.Items, 1)
	assert.Equal(t, "1", list.Items[0].ResourceVersion)
	assert.Equal(t, "testname1", list.Items[0].Name)
	assert.Equal(t, "3", list.ResourceVersion)
	assert.Equal(t, "3:1", list.Continue)

	res, err = s.List(context.Background(), "", storage.ListOptions{
		Predicate: storage.SelectionPredicate{
			Limit:    2,
			Continue: list.Continue,
		},
	})
	require.NoError(t, err)

	list = res.(*TestKindList)
	require.Len(t, list.Items, 2)
	assert.Equal(t, "2", list.Items[0].ResourceVersion)
	assert.Equal(t, "testname2", list.Items[0].Name)
	assert.Equal(t, "3", list.ResourceVersion)
	assert.Equal(t, "3", list.Items[1].ResourceVersion)
	assert.Equal(t, "testname3", list.Items[1].Name)
	assert.Equal(t, "", list.Continue)

}
