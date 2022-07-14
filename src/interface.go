package database

type Database interface {
	AddNode(node Node) error
	RemoveNode(node Node) error
	GetNodes() []Node
	FindNodeId(key []byte) int
	Write(key, value []byte) error
	Close() error
}

type Node interface {
	GetToken() int
	Write(key, value []byte) error
}

// Hasher is responsible for generating unsigned, 64bit hash of provided byte slice.
// Hasher should minimize collisions (generating same hash for different byte slice)
// and while performance is also important fast functions are preferable (i.e.
// you can use FarmHash family).
type Hasher interface {
	Sum64([]byte) uint64
}
