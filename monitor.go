package search

import (
	"github.com/infrago/base"
	"github.com/infrago/infra"
)

func (m *Module) Ready() bool {
	m.mutex.RLock()
	defer m.mutex.RUnlock()
	return m.opened && len(m.instances) > 0
}

func (m *Module) Health() infra.ModuleHealth {
	m.mutex.RLock()
	opened := m.opened
	connections := len(m.instances)
	indexes := len(m.indexes)
	m.mutex.RUnlock()
	return infra.NewModuleHealth("search", opened && connections > 0, nil, base.Map{
		"connections": connections,
		"indexes":     indexes,
	})
}

func (m *Module) Stats() infra.ModuleStats {
	m.mutex.RLock()
	opened := m.opened
	connections := len(m.instances)
	indexes := len(m.indexes)
	m.mutex.RUnlock()
	return infra.NewModuleStats("search", opened && connections > 0, base.Map{
		"connections": connections,
		"indexes":     indexes,
	})
}
