package internals

type ssTable struct {
}

func (s ssTable) Save() error {
	return nil
}

func NewSSTable() SSTable {
	return &ssTable{}
}
