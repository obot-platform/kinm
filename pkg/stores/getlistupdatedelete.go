package stores

import (
	"github.com/obot-platform/kinm/pkg/strategy"
	"k8s.io/apiserver/pkg/registry/rest"
)

var (
	_ rest.Getter             = (*GetListUpdateDeleteStore)(nil)
	_ rest.Lister             = (*GetListUpdateDeleteStore)(nil)
	_ rest.Updater            = (*GetListUpdateDeleteStore)(nil)
	_ rest.RESTDeleteStrategy = (*GetListUpdateDeleteStore)(nil)
	_ strategy.Base           = (*GetListUpdateDeleteStore)(nil)
)

type GetListUpdateDeleteStore struct {
	*strategy.SingularNameAdapter
	*strategy.GetAdapter
	*strategy.UpdateAdapter
	*strategy.ListAdapter
	*strategy.DeleteAdapter
	*strategy.DestroyAdapter
	*strategy.TableAdapter
}
