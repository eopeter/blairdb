package internals

// MemTable holds a sorted list of the latest written records
//
// Writes are duplicates of what was written to the WAL for recovery of the MemTable in the event of a restart
// MemTable has a max capacity (usually 2 MB) and when that is reached, we flush the MemTable to disk as an SSTable
type MemTable interface {
	Get(key []byte) ([]byte, error)
	Set(key, value []byte) bool
	Delete(key []byte) bool
	Size() int
	InitFromLogData(data []fileData)
	// Flush saves the MemTable as an SSTable to Disk
	Flush() error
}

type WAL interface {
	Write(key, value []byte) error
	Read() ([]fileData, error)
	Delete() bool
	Close() error
}

type SSTable interface {
	Save() error
}

type BloomFilter interface {
}
