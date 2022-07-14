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
	isLeader        bool
	address         net.IPAddr
	token           int
	memTable        internals.MemTable
	log             internals.WAL
	maxMemTableSize int
	mu              *sync.RWMutex
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
	n.mu.Lock()
	defer n.mu.Unlock()
	if _, err := os.Stat(LogFilePath); errors.Is(err, os.ErrNotExist) {
		return nil
	}
	data, err := n.log.Read()
	if err != nil {
		return err
	}
	n.memTable.InitFromLogData(data)
	return nil
}

func (n *node) GetToken() int {
	return n.token
}

func NewNode(token int, maxMemoryMb int) Node {
	n := &node{
		token:           token,
		memTable:        internals.NewMemTable(),
		maxMemTableSize: maxMemoryMb * 1048576,
		log:             internals.NewLog(LogFilePath, LogBufferSizeMb),
		mu:              &sync.RWMutex{},
	}
	go func() {
		err := n.rebuildMemTableFromWal()
		if err != nil {
			return
		}
	}()
	return n
}
