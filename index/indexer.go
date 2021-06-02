package index

import "kvstore/store"

type Indexer struct {
	// 键值信息
	Meta *store.Meta
	// entry存储文件id
	FileId  uint32
	// entry大小
	EntrySize uint32
	// 偏移地址
	Offset int64
}
