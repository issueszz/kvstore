package kvstore

import (
	"io"
	"kvstore/index"
	"kvstore/store"
	"sort"
)

const (
	String uint16 = iota
)

// 字符串相关操作类型标识符
const (
	StringSet uint16 = iota
	StringRem
)

// 字符串索引表操作
func(k *Kvstore) buildStringIndex(idx *index.Indexer, opt uint16) {
	// 检查键是否过期等等功能

	switch opt {
	case StringSet:
		k.strIndex.skl.Insert(idx.Meta.Key, idx)
	case StringRem:
		k.strIndex.skl.Remove(idx.Meta.Key)
	}
}

// 加载数据库文件
func (k  *Kvstore) loadIdxFromFiles() error {
	kvFiles := make(map[uint32]*store.KvFile)
	fileIds := make([]int, 0)

	// 遍历归档文件
	for i, v := range k.archFiles {
		fileIds = append(fileIds, int(i))
		kvFiles[i] = v
	}

	// 加入活跃文件
	kvFiles[k.activeFileId] =  k.activeFile
	fileIds = append(fileIds, int(k.activeFileId))

	// 排序文件编号
	sort.Ints(fileIds)

	// 按照文件顺序依次执行命令建立索引表
	for i := 0; i < len(fileIds); i++ {
		fid := uint32(fileIds[i])
		df :=  kvFiles[fid]
		var offset int64 = 0
		for offset <= k.config.BlockSize {
			if e, err := df.Read(offset); err == nil {
				// 根据entry  建立索引信息
				idx := &index.Indexer{
					Meta: e.Meta,
					FileId: fid,
					EntrySize: e.Size(),
					Offset: offset,
				}
				// 写入索引表
				k.buildIndex(e, idx)

				// 修改偏移
				offset += int64(e.Size())
			} else {
				if err == io.EOF {
					break
				}
				return err
			}
		}

		// 更改偏移地址
		df.Offset = offset
		if i < len(fileIds)-1 {
			if err := df.Close(true); err != nil {
				return err
			}
		}
	}

	// 返回
	return nil
}

