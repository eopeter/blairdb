package internals

import (
	"encoding/json"
	"github.com/huandu/skiplist"
	"time"
)

type memTable struct {
	entries *skiplist.SkipList
	ssTable SSTable
	size    int
}

func (m *memTable) Flush() error {
	return m.ssTable.Save()
}

func (m *memTable) InitFromLogData(data []fileData) {
	for _, d := range data {
		m.set(d.Key, d.Value, d.TimeStamp, d.Deleted)
	}
}

func (m *memTable) Size() int {
	return m.size
}

func (m *memTable) Get(key []byte) ([]byte, error) {
	result := m.entries.Get(key)
	entry, err := m.getEntry(result.Value)
	return entry.value, err
}

func (m *memTable) Set(key, value []byte) bool {
	return m.set(key, value, time.Now().UnixMilli(), false)
}

func (m *memTable) Delete(key []byte) bool {
	return m.set(key, nil, time.Now().UnixMilli(), true)
}

func (m *memTable) set(key, value []byte, timeStamp int64, delete bool) bool {
	existing := m.entries.Get(key)
	entry := memTableEntry{
		key:       key,
		value:     value,
		timestamp: timeStamp,
		deleted:   delete,
	}
	result := m.entries.Set(key, entry)
	if delete {
		m.size -= len(key)
	} else {
		if existing != nil {
			r, e := m.getEntry(existing.Value)
			if e != nil {
				m.size -= len(r.value)
				m.size += len(value)
			}
		} else {
			m.size += len(key) + len(value) + 16 + 1
		}
	}
	return result != nil
}

func (m *memTable) getEntry(result interface{}) (memTableEntry, error) {
	var entry memTableEntry
	err := json.Unmarshal(result.([]byte), &entry)
	return entry, err
}

func NewMemTable() MemTable {
	return &memTable{
		entries: skiplist.New(skiplist.Bytes),
		ssTable: NewSSTable(),
		size:    0,
	}
}
