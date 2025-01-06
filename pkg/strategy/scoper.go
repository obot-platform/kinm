package strategy

import "github.com/obot-platform/kinm/pkg/types"

type ScoperAdapter struct {
	strategy Newer
}

func NewScoper(strategy Newer) *ScoperAdapter {
	return &ScoperAdapter{
		strategy: strategy,
	}
}

func (s *ScoperAdapter) NamespaceScoped() bool {
	if s != nil {
		if o, ok := s.strategy.(types.NamespaceScoper); ok {
			return o.NamespaceScoped()
		}
		if o, ok := s.strategy.New().(types.NamespaceScoper); ok {
			return o.NamespaceScoped()
		}
	}
	return true
}
