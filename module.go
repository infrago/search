package search

import (
	"fmt"
	"sort"
	"strings"
	"sync"
	"time"

	. "github.com/infrago/base"
	"github.com/infrago/infra"
	"github.com/infrago/util"
)

func init() {
	infra.Mount(module)
}

var module = &Module{
	configs:   make(map[string]Config),
	drivers:   make(map[string]Driver),
	instances: make(map[string]*Instance),
	weights:   make(map[string]int),
	indexes:   make(map[string]Index),
}

type (
	Config struct {
		Driver  string
		Weight  int
		Prefix  string
		Timeout time.Duration
		Setting Map
	}

	Configs map[string]Config

	Instance struct {
		Name    string
		Config  Config
		Setting Map
		conn    Connection
	}

	Module struct {
		mutex sync.RWMutex

		opened bool

		configs   map[string]Config
		drivers   map[string]Driver
		instances map[string]*Instance
		weights   map[string]int
		indexes   map[string]Index
		hashring  *util.HashRing
	}
)

func (m *Module) Register(name string, value Any) {
	switch v := value.(type) {
	case Driver:
		m.RegisterDriver(name, v)
	case Config:
		m.RegisterConfig(name, v)
	case Configs:
		m.RegisterConfigs(v)
	case Index:
		m.RegisterIndex(name, v)
	case Indexes:
		m.RegisterIndexes(v)
	}
}

func (m *Module) RegisterDriver(name string, driver Driver) {
	m.mutex.Lock()
	defer m.mutex.Unlock()

	if name == "" {
		name = infra.DEFAULT
	}
	if driver == nil {
		panic("invalid search driver: " + name)
	}
	if infra.Override() {
		m.drivers[name] = driver
	} else if _, ok := m.drivers[name]; !ok {
		m.drivers[name] = driver
	}
}

func (m *Module) RegisterConfig(name string, cfg Config) {
	m.mutex.Lock()
	defer m.mutex.Unlock()

	if name == "" {
		name = infra.DEFAULT
	}
	if infra.Override() {
		m.configs[name] = cfg
	} else if _, ok := m.configs[name]; !ok {
		m.configs[name] = cfg
	}
}

func (m *Module) RegisterConfigs(configs Configs) {
	for name, cfg := range configs {
		m.RegisterConfig(name, cfg)
	}
}

func (m *Module) RegisterIndex(name string, index Index) {
	m.mutex.Lock()
	defer m.mutex.Unlock()

	if strings.TrimSpace(name) == "" {
		return
	}
	index.Name = name
	if index.Primary == "" {
		index.Primary = "id"
	}
	if infra.Override() {
		m.indexes[name] = index
	} else if _, ok := m.indexes[name]; !ok {
		m.indexes[name] = index
	}
}

func (m *Module) RegisterIndexes(indexes Indexes) {
	for name, idx := range indexes {
		m.RegisterIndex(name, idx)
	}
}

func (m *Module) Config(global Map) {
	cfgAny, ok := global["search"]
	if !ok {
		return
	}

	cfgMap, ok := cfgAny.(Map)
	if !ok || cfgMap == nil {
		return
	}

	defaults := Config{}
	if v, ok := cfgMap["driver"].(string); ok {
		defaults.Driver = v
	}
	if v, ok := cfgMap["weight"].(int); ok {
		defaults.Weight = v
	}
	if v, ok := cfgMap["prefix"].(string); ok {
		defaults.Prefix = v
	}
	if v, ok := cfgMap["timeout"]; ok {
		defaults.Timeout = parseDuration(v)
	}
	if v, ok := cfgMap["setting"].(Map); ok {
		defaults.Setting = v
	}

	if defaults.Driver != "" || defaults.Weight != 0 || defaults.Prefix != "" || defaults.Timeout > 0 || defaults.Setting != nil {
		m.RegisterConfig(infra.DEFAULT, defaults)
	}

	for name, vv := range cfgMap {
		if name == "driver" || name == "weight" || name == "prefix" || name == "timeout" || name == "setting" {
			continue
		}
		one, ok := vv.(Map)
		if !ok {
			continue
		}
		cfg := Config{}
		if v, ok := one["driver"].(string); ok {
			cfg.Driver = v
		}
		if v, ok := one["weight"].(int); ok {
			cfg.Weight = v
		}
		if v, ok := one["prefix"].(string); ok {
			cfg.Prefix = v
		}
		if v, ok := one["timeout"]; ok {
			cfg.Timeout = parseDuration(v)
		}
		if v, ok := one["setting"].(Map); ok {
			cfg.Setting = v
		}
		m.RegisterConfig(name, cfg)
	}
}

func (m *Module) Setup() {}

func (m *Module) Open() {
	m.mutex.Lock()
	defer m.mutex.Unlock()

	if m.opened {
		return
	}

	if len(m.configs) == 0 {
		m.configs[infra.DEFAULT] = Config{Driver: infra.DEFAULT, Weight: 1}
	}

	for name, cfg := range m.configs {
		if name == "" {
			name = infra.DEFAULT
		}
		if cfg.Driver == "" {
			cfg.Driver = infra.DEFAULT
		}
		if cfg.Weight == 0 {
			cfg.Weight = 1
		}
		m.configs[name] = cfg
	}

	for name, cfg := range m.configs {
		drv := m.drivers[cfg.Driver]
		if drv == nil {
			panic("missing search driver: " + cfg.Driver)
		}
		inst := &Instance{Name: name, Config: cfg, Setting: cfg.Setting}
		conn, err := drv.Connect(inst)
		if err != nil {
			panic("connect search failed: " + err.Error())
		}
		if err := conn.Open(); err != nil {
			panic("open search failed: " + err.Error())
		}
		inst.conn = conn
		m.instances[name] = inst
		m.weights[name] = cfg.Weight
	}

	m.hashring = util.NewHashRing(m.weights)

	for name, index := range m.indexes {
		conn := m.pickConnLocked(name)
		if conn == nil {
			continue
		}
		if err := conn.SyncIndex(name, index); err != nil {
			panic("create search index failed: " + err.Error())
		}
	}
	m.opened = true
}

func (m *Module) Start() {
	infra.Log(infra.LogLevelInfo, "search", "module started", Map{"connections": len(m.instances)})
}

func (m *Module) Stop() {}

func (m *Module) Close() {
	m.mutex.Lock()
	defer m.mutex.Unlock()

	for _, inst := range m.instances {
		if inst.conn != nil {
			_ = inst.conn.Close()
		}
	}
	m.instances = make(map[string]*Instance)
	m.weights = make(map[string]int)
	m.hashring = nil
	m.opened = false
}

func (m *Module) pickConn(key string) Connection {
	m.mutex.RLock()
	defer m.mutex.RUnlock()
	return m.pickConnLocked(key)
}

func (m *Module) pickConnLocked(key string) Connection {
	if len(m.instances) == 0 {
		return nil
	}
	if m.hashring == nil {
		for _, inst := range m.instances {
			return inst.conn
		}
		return nil
	}
	name := m.hashring.Locate(key)
	if inst, ok := m.instances[name]; ok {
		return inst.conn
	}
	for _, inst := range m.instances {
		return inst.conn
	}
	return nil
}

func (m *Module) Clear(index string) error {
	index = strings.TrimSpace(index)
	if index == "" {
		return fmt.Errorf("search index is empty")
	}
	conn := m.pickConn(index)
	if conn == nil {
		return fmt.Errorf("search is not ready")
	}
	return conn.Clear(index)
}

func (m *Module) Capabilities(index string) Capabilities {
	conn := m.pickConn(index)
	if conn == nil {
		return Capabilities{}
	}
	return conn.Capabilities()
}

func (m *Module) ListCapabilities() map[string]Capabilities {
	out := map[string]Capabilities{}
	m.mutex.RLock()
	defer m.mutex.RUnlock()
	for name, inst := range m.instances {
		if inst == nil || inst.conn == nil {
			continue
		}
		out[name] = inst.conn.Capabilities()
	}
	return out
}

func (m *Module) Upsert(index string, rows ...Map) error {
	conn := m.pickConn(index)
	if conn == nil {
		return fmt.Errorf("search is not ready")
	}
	rows, err := m.prepareRows(index, rows)
	if err != nil {
		return err
	}
	return conn.Upsert(index, rows)
}

func (m *Module) Delete(index string, ids []string) error {
	conn := m.pickConn(index)
	if conn == nil {
		return fmt.Errorf("search is not ready")
	}
	return conn.Delete(index, ids)
}

func (m *Module) Search(index, keyword string, args ...Any) (Result, error) {
	conn := m.pickConn(index)
	if conn == nil {
		return Result{}, fmt.Errorf("search is not ready")
	}
	query := BuildQuery(keyword, args...)
	res, err := conn.Search(index, query)
	if err != nil {
		return res, err
	}
	return m.normalizeResult(index, res)
}

func (m *Module) Count(index, keyword string, args ...Any) (int64, error) {
	conn := m.pickConn(index)
	if conn == nil {
		return 0, fmt.Errorf("search is not ready")
	}
	query := BuildQuery(keyword, args...)
	return conn.Count(index, query)
}

func (m *Module) prepareRows(index string, rows []Map) ([]Map, error) {
	m.mutex.RLock()
	idx, ok := m.indexes[index]
	m.mutex.RUnlock()
	if !ok {
		idx = Index{}
	}
	pk := idx.Primary
	if pk == "" {
		pk = "id"
	}
	strictWrite := idx.StrictWrite
	if !ok || len(idx.Attributes) == 0 {
		out := make([]Map, 0, len(rows))
		for _, row := range rows {
			payload := clonePayload(row)
			if payload == nil {
				payload = Map{}
			}
			idValue, ok := payload[pk]
			if !ok || fmt.Sprintf("%v", idValue) == "" {
				idValue, ok = payload["id"]
				if !ok || fmt.Sprintf("%v", idValue) == "" {
					return nil, fmt.Errorf("search index %s missing primary key %s", index, pk)
				}
			}
			payload[pk] = idValue
			payload["id"] = idValue
			out = append(out, payload)
		}
		return out, nil
	}

	out := make([]Map, 0, len(rows))
	for _, row := range rows {
		payload := clonePayload(row)
		if payload == nil {
			payload = Map{}
		}
		idValue, ok := payload[pk]
		if !ok || fmt.Sprintf("%v", idValue) == "" {
			idValue, ok = payload["id"]
			if !ok || fmt.Sprintf("%v", idValue) == "" {
				return nil, fmt.Errorf("search index %s missing primary key %s", index, pk)
			}
		}
		payload[pk] = idValue
		payload["id"] = idValue

		wrapped := Map{}
		res := infra.Mapping(idx.Attributes, payload, wrapped, false, !strictWrite)
		if res != nil && res.Fail() {
			return nil, fmt.Errorf("search index %s mapping failed: %s", index, res.Error())
		}

		if len(wrapped) > 0 {
			payload = wrapped
		}
		payload[pk] = idValue
		payload["id"] = idValue
		out = append(out, payload)
	}
	return out, nil
}

func (m *Module) normalizeResult(index string, result Result) (Result, error) {
	m.mutex.RLock()
	idx, ok := m.indexes[index]
	m.mutex.RUnlock()
	if !ok || len(idx.Attributes) == 0 {
		return result, nil
	}

	strictRead := idx.StrictRead
	for i := range result.Hits {
		payload := result.Hits[i].Payload
		if payload == nil {
			continue
		}
		wrapped := Map{}
		res := infra.Mapping(idx.Attributes, payload, wrapped, false, !strictRead)
		if res != nil && res.Fail() {
			if strictRead {
				return result, fmt.Errorf("search index %s read mapping failed: %s", index, res.Error())
			}
			continue
		}
		if len(wrapped) > 0 {
			result.Hits[i].Payload = wrapped
		}
	}
	return result, nil
}

func parseDuration(v Any) time.Duration {
	switch vv := v.(type) {
	case time.Duration:
		return vv
	case int:
		return time.Second * time.Duration(vv)
	case int64:
		return time.Second * time.Duration(vv)
	case float64:
		return time.Second * time.Duration(vv)
	case string:
		d, err := time.ParseDuration(vv)
		if err == nil {
			return d
		}
	}
	return 0
}

func mergeMaps(ms ...Map) Map {
	out := Map{}
	for _, m := range ms {
		if m == nil {
			continue
		}
		for k, v := range m {
			out[k] = v
		}
	}
	return out
}

func toStrings(v Any) []string {
	switch vv := v.(type) {
	case string:
		if vv == "" {
			return []string{}
		}
		arr := stringsSplit(vv)
		return arr
	case []string:
		return vv
	case []Any:
		out := make([]string, 0, len(vv))
		for _, one := range vv {
			out = append(out, fmt.Sprintf("%v", one))
		}
		return out
	default:
		return []string{}
	}
}

func stringsSplit(v string) []string {
	parts := make([]string, 0)
	cur := ""
	for _, ch := range v {
		if ch == ',' || ch == ';' || ch == ' ' || ch == '\n' || ch == '\t' {
			if cur != "" {
				parts = append(parts, cur)
				cur = ""
			}
			continue
		}
		cur += string(ch)
	}
	if cur != "" {
		parts = append(parts, cur)
	}
	return parts
}

func mapKeys(m map[string]int64) []string {
	out := make([]string, 0, len(m))
	for k := range m {
		out = append(out, k)
	}
	sort.Strings(out)
	return out
}

func clonePayload(src Map) Map {
	if src == nil {
		return nil
	}
	out := Map{}
	for k, v := range src {
		out[k] = v
	}
	return out
}
