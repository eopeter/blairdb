package database

import (
	"errors"
	"github.com/eopeter/blairdb/src/internals"
	"net"
	"os"
	"sync"
)

const (
	LogFilePath     = "/users/eopeter/blairdb/log.blair"
	LogBufferSizeMb = 1
)

type node struct {
	name            string
	isLeader        bool
	address         net.IPAddr
	token           int
	log             internals.WAL
	memTable        internals.MemTable
	ssTable         internals.SSTable
	bloomFilter     internals.BloomFilter
	maxMemTableSize int
	mu              *sync.RWMutex
}

func (n *node) String() string {
	return n.name
}

func (n *node) Read(key []byte) ([]byte, error) {
	// check Bloom Filter
	if !n.bloomFilter.HasKey(key) {
		return nil, nil
	}
	// check if in memTable
	b, e := n.memTable.Get(key)
	if b != nil && e == nil {
		return b, nil
	}
	// check if in SSTable
	return nil, nil
}

func (n *node) Write(key, value []byte) error {
	n.mu.Lock()
	defer n.mu.Unlock()
	// check size of MemTable.
	if n.memTable.Size() >= n.maxMemTableSize {
		// flush to SSTable
		n.memTable.Flush()
		// delete WAL
		n.log.Delete()
		// reset MemTable
		n.memTable = internals.NewMemTable()
		n.log = internals.NewLog(LogFilePath, LogBufferSizeMb)
	}
	// append to WAL
	walErr := n.log.Write(key, value)
	if walErr != nil {
		return walErr
	}
	// Write to MemTable
	n.memTable.Set(key, value)
	// Add to Bloom Filter
	n.bloomFilter.Add(key)
	return nil
}

func (n *node) Close() error {
	n.mu.Lock()
	defer n.mu.Unlock()
	err := n.log.Close()
	if err != nil {
		return err
	}
	err = n.memTable.Flush()
	if err != nil {
		return err
	}
	return nil
}

func (n *node) rebuildMemTableFromWal() error {
	if _, err := os.Stat(LogFilePath); errors.Is(err, os.ErrNotExist) {
		return nil
	}
	data, err := n.log.Read()
	if err != nil {
		return err
	}
	if data != nil {
		n.memTable.InitFromLogData(data)
	}
	return nil
}

func (n *node) rebuildBloomFilterFromSSTable() error {
	return nil
}

func (n *node) GetToken() int {
	return n.token
}

func NewNode(token int, maxMemoryMb int) Node {
	n := &node{
		token:           token,
		maxMemTableSize: maxMemoryMb * 1048576,
		log:             internals.NewLog(LogFilePath, LogBufferSizeMb),
		memTable:        internals.NewMemTable(),
		bloomFilter:     internals.NewBloomFilter(18),
		ssTable:         internals.NewSSTable(),
		mu:              &sync.RWMutex{},
	}
	go func() {
		err := n.rebuildMemTableFromWal()
		if err != nil {
			return
		}
	}()

	go func() {
		err := n.rebuildBloomFilterFromSSTable()
		if err != nil {
			return
		}
	}()
	return n
}
