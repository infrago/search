package search

import (
	"encoding/json"
	"fmt"
	"sort"
	"strings"
	"sync"
	"time"

	. "github.com/infrago/base"
	"github.com/infrago/infra"
)

type defaultDriver struct{}

type defaultConnection struct {
	mutex   sync.RWMutex
	indexes map[string]*memoryIndex
}

type memoryIndex struct {
	name string
	docs map[string]Map
}

func init() {
	module.RegisterDriver(infra.DEFAULT, &defaultDriver{})
}

func (d *defaultDriver) Connect(inst *Instance) (Connection, error) {
	return &defaultConnection{indexes: make(map[string]*memoryIndex)}, nil
}

func (c *defaultConnection) Open() error  { return nil }
func (c *defaultConnection) Close() error { return nil }
func (c *defaultConnection) Capabilities() Capabilities {
	return Capabilities{
		SyncIndex: true,
		Clear:     true,
		Upsert:    true,
		Delete:    true,
		Search:    true,
		Count:     true,
		Suggest:   false,
		Sort:      true,
		Facets:    true,
		Highlight: true,
		FilterOps: []string{OpEq, OpNe, OpIn, OpNin, OpGt, OpGte, OpLt, OpLte, OpRange},
	}
}

func (c *defaultConnection) SyncIndex(name string, index Index) error {
	c.mutex.Lock()
	defer c.mutex.Unlock()
	if _, ok := c.indexes[name]; !ok {
		c.indexes[name] = &memoryIndex{name: name, docs: map[string]Map{}}
	}
	return nil
}

func (c *defaultConnection) Clear(name string) error {
	c.mutex.Lock()
	defer c.mutex.Unlock()
	if idx, ok := c.indexes[name]; ok && idx != nil {
		idx.docs = map[string]Map{}
	}
	return nil
}

func (c *defaultConnection) ensure(index string) *memoryIndex {
	if idx, ok := c.indexes[index]; ok {
		return idx
	}
	idx := &memoryIndex{name: index, docs: map[string]Map{}}
	c.indexes[index] = idx
	return idx
}

func (c *defaultConnection) Upsert(index string, rows []Map) error {
	c.mutex.Lock()
	defer c.mutex.Unlock()
	idx := c.ensure(index)
	for _, row := range rows {
		if row == nil {
			continue
		}
		id := fmt.Sprintf("%v", row["id"])
		if id == "" || id == "<nil>" {
			continue
		}
		idx.docs[id] = cloneMap(row)
	}
	return nil
}

func (c *defaultConnection) Delete(index string, ids []string) error {
	c.mutex.Lock()
	defer c.mutex.Unlock()
	idx := c.ensure(index)
	for _, id := range ids {
		delete(idx.docs, id)
	}
	return nil
}

func (c *defaultConnection) Search(index string, query Query) (Result, error) {
	start := time.Now()

	c.mutex.RLock()
	idx := c.indexes[index]
	c.mutex.RUnlock()
	if idx == nil {
		return Result{Hits: []Hit{}, Facets: map[string][]Facet{}}, nil
	}

	matched := make([]Hit, 0)
	keyword := strings.ToLower(strings.TrimSpace(query.Keyword))

	for id, payload := range idx.docs {
		if !defaultKeywordMatch(keyword, payload, query.Prefix) {
			continue
		}
		ok := true
		for _, f := range query.Filters {
			if !FilterMatch(f, payload) {
				ok = false
				break
			}
		}
		if !ok {
			continue
		}
		matched = append(matched, Hit{ID: id, Score: 1.0, Payload: cloneMap(payload)})
	}

	if len(query.Sorts) > 0 {
		sort.SliceStable(matched, func(i, j int) bool {
			for _, s := range query.Sorts {
				ai := matched[i].Payload[s.Field]
				aj := matched[j].Payload[s.Field]
				cmp := compareForSort(ai, aj)
				if cmp == 0 {
					continue
				}
				if s.Desc {
					return cmp > 0
				}
				return cmp < 0
			}
			return matched[i].ID < matched[j].ID
		})
	}

	facets := map[string][]Facet{}
	if len(query.Facets) > 0 {
		for _, field := range query.Facets {
			counter := map[string]int64{}
			for _, hit := range matched {
				counter[fmt.Sprintf("%v", hit.Payload[field])]++
			}
			keys := mapKeys(counter)
			vals := make([]Facet, 0, len(keys))
			for _, k := range keys {
				vals = append(vals, Facet{Field: field, Value: k, Count: counter[k]})
			}
			facets[field] = vals
		}
	}

	total := int64(len(matched))
	offset := query.Offset
	if offset < 0 {
		offset = 0
	}
	limit := query.Limit
	if limit <= 0 {
		limit = 20
	}
	if offset > len(matched) {
		offset = len(matched)
	}
	end := offset + limit
	if end > len(matched) {
		end = len(matched)
	}
	hits := matched[offset:end]

	if len(query.Fields) > 0 {
		for i := range hits {
			hits[i].Payload = pickFields(hits[i].Payload, query.Fields)
		}
	}

	if keyword != "" && len(query.Highlight) > 0 {
		for i := range hits {
			for _, field := range query.Highlight {
				if raw, ok := hits[i].Payload[field]; ok {
					text := fmt.Sprintf("%v", raw)
					lower := strings.ToLower(text)
					if pos := strings.Index(lower, keyword); pos >= 0 {
						endPos := pos + len(keyword)
						hits[i].Payload[field] = text[:pos] + "<em>" + text[pos:endPos] + "</em>" + text[endPos:]
					}
				}
			}
		}
	}

	return Result{Total: total, Took: time.Since(start).Milliseconds(), Hits: hits, Facets: facets}, nil
}

func (c *defaultConnection) Count(index string, query Query) (int64, error) {
	query.Offset = 0
	query.Limit = 1
	res, err := c.Search(index, query)
	if err != nil {
		return 0, err
	}
	return res.Total, nil
}

func defaultKeywordMatch(keyword string, payload Map, prefix bool) bool {
	if keyword == "" {
		return true
	}
	bts, _ := json.Marshal(payload)
	data := strings.ToLower(string(bts))
	if prefix {
		return strings.HasPrefix(data, keyword)
	}
	return strings.Contains(data, keyword)
}

func compareForSort(a, b Any) int {
	if fa, oka := toFloat(a); oka {
		if fb, okb := toFloat(b); okb {
			switch {
			case fa < fb:
				return -1
			case fa > fb:
				return 1
			default:
				return 0
			}
		}
	}
	sa := fmt.Sprintf("%v", a)
	sb := fmt.Sprintf("%v", b)
	switch {
	case sa < sb:
		return -1
	case sa > sb:
		return 1
	default:
		return 0
	}
}

func pickFields(payload Map, fields []string) Map {
	if payload == nil {
		return Map{}
	}
	out := Map{}
	for _, field := range fields {
		if v, ok := payload[field]; ok {
			out[field] = v
		}
	}
	if len(out) == 0 {
		return cloneMap(payload)
	}
	return out
}

func cloneMap(m Map) Map {
	if m == nil {
		return nil
	}
	out := Map{}
	for k, v := range m {
		out[k] = v
	}
	return out
}
