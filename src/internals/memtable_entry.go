package internals

// memTableEntry holds the actual record entered into the memTable
type memTableEntry struct {
	// key for the value being stored for this record
	// records in the memTable are sorted by the key
	key []byte
	// value being stored for this record
	value []byte
	// timestamp is the time our write occurred in microseconds
	// it is used to order writes to the same key when cleaning old data in SSTables
	timestamp int64
	// Delete is a Tombstone value that indicates if the record is marked for deletion
	deleted bool
}
