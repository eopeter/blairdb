package internals

import (
	"bufio"
	"encoding/json"
	"hash"
	"hash/crc32"
	"log"
	"os"
	"sync"
	"time"
)

type fileData struct {
	KeySize   int    `json:"keySize"`
	ValueSize int    `json:"valueSize"`
	Key       []byte `json:"key"`
	Value     []byte `json:"value"`
	TimeStamp int64  `json:"timeStamp"`
	Deleted   bool   `json:"deleted"`
}

type wal struct {
	filePath   string
	writer     *bufio.Writer
	reader     *bufio.Scanner
	bufferSize int
	hasher     hash.Hash32
	segments   []wal
	mu         *sync.RWMutex
}

func (w *wal) Close() error {
	return w.writer.Flush()
}

func (w *wal) Write(key, value []byte) error {
	data := fileData{
		KeySize:   len(key),
		ValueSize: len(value),
		Key:       key,
		Value:     value,
		TimeStamp: time.Now().UnixMilli(),
		Deleted:   false,
	}
	dataBytes, err := json.Marshal(data)
	if err != nil {
		return err
	}
	_, err = w.writer.WriteString(string(dataBytes) + "\n")
	if err != nil {
		return err
	}
	// Cyclic Redundancy Check (CRC) value for data validation
	_, err = w.hasher.Write(dataBytes)
	if err != nil {
		// failed to update the hash
		return err
	}
	err = w.writer.Flush()
	if err != nil {
		return err
	}
	return nil
}

func (w *wal) Read() ([]fileData, error) {
	var result []fileData
	for w.reader.Scan() {
		var data fileData
		b := w.reader.Bytes()
		err := json.Unmarshal(b, &data)
		if err != nil {
			return nil, err
		}
		result = append(result, data)
	}
	return result, nil
}

func (w *wal) Delete() bool {
	//TODO implement me
	panic("implement me")
}

func NewLog(path string, bufferSizeMb int) WAL {
	f, err := os.OpenFile(path, os.O_RDWR|os.O_CREATE|os.O_APPEND, 0600)
	if err != nil {
		log.Fatal(err)
	}
	bufferSize := bufferSizeMb * 1048576
	scanner := bufio.NewScanner(f)
	scanner.Split(bufio.ScanLines)
	readBuffer := make([]byte, bufferSize)
	scanner.Buffer(readBuffer, bufferSize)
	return &wal{
		filePath:   path,
		bufferSize: bufferSize,
		writer:     bufio.NewWriterSize(f, bufferSize),
		reader:     scanner,
		hasher:     crc32.New(crc32.MakeTable(crc32.Castagnoli)),
	}
}
