package database

import (
	"fmt"
	"hash/fnv"
	"sync"
)

type blairDb struct {
	nodes             map[int]Node
	nodeCount         int
	replicationFactor int
	hasher            Hasher
	maxMemTableSize   int
	mu                *sync.RWMutex
}

func (b *blairDb) GetNodes() []Node {
	//TODO implement me
	panic("implement me")
}

func (b *blairDb) FindNodeId(key []byte) int {
	hashKey := b.hasher.Sum64(key)
	return int(hashKey % uint64(b.nodeCount))
}

func (b *blairDb) RemoveNode(node Node) error {
	b.mu.Lock()
	defer b.mu.Unlock()
	delete(b.nodes, node.GetToken())
	return nil
}

func (b *blairDb) AddNode(node Node) error {
	b.mu.Lock()
	defer b.mu.Unlock()
	b.nodes[node.GetToken()] = node
	return nil
}

func (b *blairDb) Write(key, value []byte) error {
	nodeId := b.FindNodeId(key)
	node := b.nodes[nodeId]
	if node == nil {
		return fmt.Errorf("no node found for hashing key: %d", nodeId)
	}
	return node.Write(key, value)
}

func (b *blairDb) Close() error {
	return nil
}

func (b *blairDb) distributePartitions() {

}

// New creates a new instance of the Database
func New(replicationFactor, nodeCount, maxMemoryMb int) Database {
	db := &blairDb{
		maxMemTableSize:   maxMemoryMb * 1048576,
		mu:                &sync.RWMutex{},
		nodes:             make(map[int]Node),
		nodeCount:         nodeCount,
		replicationFactor: replicationFactor,
		hasher:            hashFunc{},
	}
	return db
}

type hashFunc struct {
}

func (hs hashFunc) Sum64(data []byte) uint64 {
	h := fnv.New64()
	h.Write(data)
	return h.Sum64()
}
