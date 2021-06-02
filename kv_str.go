package kvstore

import (
	"kvstore/index"
	"kvstore/store"
	"log"
	"sync"
	"time"
)

type StrIdx struct {
	skl *index.SkipList
	mu  sync.RWMutex
}

// NewStrIdx 建立字符串索引表
func NewStrIdx() *StrIdx {
	return &StrIdx{skl: index.InitSkl()}
}

// Set 设置str值
func (k *Kvstore) Set(key []byte, value []byte) error {
	//检查数据是否合法
	if err := k.checkKeyValue(key, value); err != nil {
		return err
	}

	// 加锁
	k.strIndex.mu.Lock()
	defer k.strIndex.mu.Unlock()

	elem := k.strIndex.skl.Find(key)
	if elem == nil {
		err := k.doSet(key, value)
		return err
	}
	if _, ok := k.expires[string(key)]; ok {
		// 删除过期键
		delete(k.expires, string(key))
	}
	return nil
}
// Get 获取str数据
func (k *Kvstore) Get(key []byte) ([]byte, error) {
	// 判断key是否合法
	if err := k.checkKeyValue(key, nil); err != nil {
		return nil, err
	}

	expire := k.isExpired(key)

	// 根据是否过期来选择加不同的锁
	if expire {
		k.strIndex.mu.Lock()
		defer k.strIndex.mu.Unlock()
	} else {
		k.strIndex.mu.RLock()
		defer k.strIndex.mu.RUnlock()
	}

	if expire {
		delete(k.expires, string(key))
		ele := k.strIndex.skl.Find(key)
		if ele == nil {
			return nil, ErrKeyNotExist
		}
		e := store.NewNoExtraEntry(key, nil, String, StringRem)
		if err := k.store(e); err != nil {
			log.Printf("remove expires %s, %s\n", key, err.Error())
		}
		return nil, ErrKeyNotExist
	}

	// 从索引表获取数据
	element := k.strIndex.skl.Find(key)
	if element == nil {
		return nil, ErrKeyNotExist
	}

	// 类型断言
	idx := element.Value().(*index.Indexer)
	if idx == nil {
		return nil, ErrNilIndexer
	}

	// 键值都存在内存中
	if k.config.IdxMode == KeyValueMode {
		return idx.Meta.Value, nil
	}

	// 只有键存在内存中, 则需要到磁盘中寻找
	if k.config.IdxMode == OnlyKeyMode {
		kf := k.activeFile
		if idx.FileId != kf.Id {
			kf = k.archFiles[idx.FileId]
		}

		e, err := kf.Read(idx.Offset)
		if err != nil {
			return nil, err
		}
		return e.Meta.Value, nil
	}

	// 返回
	return nil, ErrKeyNotExist
}

// StrRem 根据给定的key删除索引表中的数据
func (k *Kvstore) StrRem(key []byte) error {
	// 检查数据是否合法
	if err := k.checkKeyValue(key, nil); err != nil {
		return err
	}
	// 加锁
	k.strIndex.mu.Lock()
	defer k.strIndex.mu.Unlock()

	// 删除操作
	if item := k.strIndex.skl.Remove(key); item != nil {
		// 过期字典处理
		delete(k.expires, string(key))
		// 封装entry 然后写入文件
		e := store.NewNoExtraEntry(key, nil, String, StringRem)

		if err := k.store(e); err != nil {
			return err
		}
	}

	// 返回
	return nil
}

// Expire 设置过期时间
func (k *Kvstore) Expire(key []byte, seconds uint64) (err error) {
	// 检查键值是否合法
	err = k.checkKeyValue(key, nil)
	if err != nil {
		return err
	}

	// 加锁
	k.strIndex.mu.Lock()
	defer k.strIndex.mu.Unlock()

	// 判断索引表是否存在键值对
	ele := k.strIndex.skl.Find(key)
	if ele == nil {
		return ErrKeyNotExist
	}

	// 更新过期时间
	deadline := uint64(time.Now().Unix()) + seconds
	k.expires[string(key)] = deadline
	return
}
// TTL 获得存活时间
func (k *Kvstore) TTL(key []byte) (uint64, error) {
	// 检查键值是否合法
	err := k.checkKeyValue(key, nil)
	if err != nil {
		return 0, err
	}
	// 加锁
	k.strIndex.mu.RLock()
	defer k.strIndex.mu.RUnlock()

	// 判断索引表是否存在键值对
	ele := k.strIndex.skl.Find(key)
	if ele == nil {
		return 0, ErrKeyNotExist
	}

	deadline, exist := k.expires[string(key)]
	if !exist {
		return 0, ErrKeyIsPermanent
	}

	now := uint64(time.Now().Unix())
	remain := deadline - now
	if remain <= 0 {
		return 0, ErrKeyHasExpired
	}
	return remain, nil
}

// 建立索引信息并且将操作写入文件
func (k *Kvstore) doSet(key, value []byte) error {
	// 封装成entry
	e := store.NewNoExtraEntry(key, value, String, StringSet)

	// 写入文件
	if err := k.store(e); err != nil {
		return err
	}

	// 封装成写入索引表的数据
	idx := &index.Indexer{
		Meta: &store.Meta{Key: key, KeySize: uint32(len(key))},
		FileId: k.activeFileId,
		EntrySize: e.Size(),
		Offset: k.activeFile.Offset-int64(e.Size()),
	}

	// 键值模式
	if k.config.IdxMode == KeyValueMode {
		idx.Meta.Value = e.Meta.Value
		idx.Meta.ValueSize = e.Meta.ValueSize
	}

	// 更新索引表信息
	k.strIndex.skl.Insert(idx.Meta.Key, idx)

	// 返回
	return nil
}

// 判断是否过期
func (k *Kvstore) isExpired(key []byte) bool {
	k.strIndex.mu.RLock()
	defer k.strIndex.mu.RUnlock()
	now := time.Now().Unix()
	if k.expires[string(key)] <= 0 {
		return false
	}
	return k.expires[string(key)] <= uint64(now)
}



