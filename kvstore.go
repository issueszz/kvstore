package kvstore

import (
	"errors"
	"fmt"
	"io"
	"kvstore/index"
	"kvstore/store"
	"os"
	"sync"
)

var (
	// ErrEmptyKey 键为空
	ErrEmptyKey = errors.New("kvstore: the key is empty")

	// ErrKeyNotExist 键不存在
	ErrKeyNotExist = errors.New("kvstore: key is not exist")

	// ErrNilIndexer 索引信息为空
	ErrNilIndexer = errors.New("kvstore: Indexer is nil")

	// ErrKeyTooLarge 键太长
	ErrKeyTooLarge = errors.New("kvstore: key is too long")

	// ErrValueTooLarge 值太长
	ErrValueTooLarge = errors.New("kvstore: value is too long")

	// ErrLessThanReWriteThreshold 归档文件数量未达到重写阈值
	ErrLessThanReWriteThreshold = errors.New("kvstore: the size of archfiles less than threshold")

	// ErrKeyValueHasExisted 键值已经存在
	ErrKeyValueHasExisted = errors.New("kvstore: key ans value has existed")

	// ErrKeyIsPermanent key是永久的
	ErrKeyIsPermanent = errors.New("kvstore: the key is Permanent")

	// ErrKeyHasExpired 该键已经过期
	ErrKeyHasExpired = errors.New("the key has expired")

)

const (
	// 重写数据文件临时目录
	rewritePath = "/tmp/kvStore/Rewrite"

	// 过期字典目录
	expiresPath = "/tmp/kvStore/expires.data"
)

type Kvstore struct {
	// 当前活跃文件
	activeFile *store.KvFile
	// 当前活跃文件id
	activeFileId uint32
	// 归档文件
	archFiles map[uint32]*store.KvFile
	// 字符串索引表
	strIndex *StrIdx
	// 数据库配置信息
	config *Config
	// 读写锁
	mu sync.RWMutex
	// 过期字典
	expires store.Expires
}

// Open 初始化数据库
func Open(config *Config) (*Kvstore, error) {
	if _, err := os.Stat(config.DirPath); os.IsNotExist(err) {
		err = os.MkdirAll(config.DirPath, os.ModePerm)
		if err != nil {
			return nil, err
		}
	}
	//加载数据文件
	archFiles, activeFileId, err := store.Build(config.DirPath, config.Method)
	if err != nil {
		return nil, err
	}
	// 建立当前活跃文件
	file, err := store.NewKvFile(config.DirPath, activeFileId, config.Method)
	if err != nil {
		return nil, err
	}
	// 加载过期字典
	expires, err := store.LoadExpires(expiresPath)
	if err != nil {
		return nil, err
	}
	// 加载额外信息

	// 初始化数据库
	kv := &Kvstore{
		activeFile: file,
		activeFileId: activeFileId,
		archFiles: archFiles,
		strIndex: NewStrIdx(),
		config: config,
		expires: expires,
	}

	//启动数据库时， 加载数据库文件
	if err := kv.loadIdxFromFiles(); err != nil {
		return nil, err
	}

	// 返回
	return kv, nil
}

// Close 关闭数据库
func (k *Kvstore) Close() error {
	// 加锁
	k.mu.Lock()
	defer k.mu.Unlock()

	// 保存配置文件

	// 保存额外信息

	// 保存过期字典
	if err := k.expires.SaveExpires(expiresPath); err != nil {
		return err
	}
	// 关闭当前活跃文件
	if err := k.activeFile.Close(true);  err != nil {
		return err
	}
	// 返回
	return nil
}

// Sync 将缓存中数据同步到磁盘
func (k *Kvstore) Sync() error {
	// 加锁
	k.mu.Lock()
	defer k.mu.Unlock()

	// 对当前活跃文件同步到磁盘
	if err := k.activeFile.Sync(); err != nil {
		return err
	}

	// 返回
	return nil
}

// 检查数据是否合法
func (k *Kvstore) checkKeyValue(key []byte, value ...[]byte) error {
	// 检查键是否为空
	if len(key) == 0 {
		return ErrEmptyKey
	}
	// 检查键长度是否符合标准
	if uint32(len(key)) > k.config.MaxKeySize {
		return ErrKeyTooLarge
	}
	// 检查值长度是否符合标准
	for _, v := range value {
		if uint32(len(v)) > k.config.MaxValueSize {
			return ErrValueTooLarge
		}
	}
	return nil
}

// BuildIndex 向索引表中写入数据, 主要作用是初始化数据库时加载磁盘中的entry， 然后建立相应的索引表信息
func (k *Kvstore) buildIndex(e *store.Entry, idx *index.Indexer) {
	// 根据配置信息选择是否将值写入索引表
	if k.config.IdxMode == KeyValueMode {
		idx.Meta.Value = e.Meta.Value
		idx.Meta.ValueSize = uint32(len(idx.Meta.Value))
	}

	switch e.Type {
	case String:
		// 针对字符串的写入操作
		k.buildStringIndex(idx, e.Mark)
	}
}

// 将命令和数据写入磁盘
func(k *Kvstore) store(e *store.Entry) error {
	//配置信息
	config := k.config
	// 判断当前活跃文件剩余的大小是否满足写入新的数据条
	if k.activeFile.Offset+int64(e.Size()) > config.BlockSize {
		//将当前文件同步到外存， 关闭当前文件
		if err := k.activeFile.Close(true); err != nil {
			return err
		}
		// 归档
		k.archFiles[k.activeFileId] = k.activeFile

		// 打开新的文件
		file, err := store.NewKvFile(config.DirPath, k.activeFileId+1, config.Method)
		if err != nil {
			return err
		}

		// 更新数据库
		k.activeFile = file
		k.activeFileId++
	}
	// 将数据条写入当前活跃文件
	if err := k.activeFile.Write(e); err != nil {
		return err
	}

	// 根据配置是否立即同步外存
	if config.Sync {
		if err := k.activeFile.Sync(); err != nil {
			return err
		}
	}
	// 返回
	return nil
}

// Rewrite 重写归档数据库文件， 删除冗余数据
func (k *Kvstore) Rewrite() error {
	//判断归档文件数量是否达到重写阈值
	if len(k.archFiles) < k.config.ReWriteThreshold {
		return ErrLessThanReWriteThreshold
	}
	// 新建重写目录
	rewrites := k.config.DirPath + rewritePath
	err := os.MkdirAll(rewrites, os.ModePerm)
	if err != nil {
		return err
	}
	defer os.RemoveAll(rewrites)

	// 加锁
	k.mu.Lock()
	defer k.mu.Unlock()

	var (
		newArchFiles = make(map[uint32]*store.KvFile)
		activeFileId uint32 = 0
		df *store.KvFile
	)

	k.archFiles[k.activeFileId] = k.activeFile
	// 整理归档文件中的数据条entry
	for _, f := range k.archFiles {
		var newEntries []*store.Entry
		var offset int64 = 0
		kf, err := os.Open(f.File.Name())
		if err != nil {
			return err
		}
		f.File = kf
		for offset <= k.config.BlockSize {
			if e, err := f.Read(offset); err == nil {
				// 判断数据条是否满足重写条件
				if k.validEntry(e) {
					newEntries = append(newEntries, e)
				}
				offset += int64(e.Size())
			} else {
				if err == io.EOF {
					break
				}
				return err
			}
		}

		if len(newEntries) > 0 {
			for _, e := range newEntries {
				// 判断当前文件是否为空， 或者剩余位置是否满足写入
				if df == nil || (df.Offset + int64(e.Size())) > k.config.BlockSize {
					df, err = store.NewKvFile(rewrites, activeFileId, k.config.Method)
					if err != nil {
						return err
					}
					// 归档
					newArchFiles[activeFileId] = df
					activeFileId++
				}
				// 将entry写入新文件中
				if err := df.Write(e); err != nil {
					return err
				}

				//更新索引表信息
				if e.Type == String {
					node := k.strIndex.skl.Find(e.Meta.Key)
					v := node.Value().(*index.Indexer)
					v.Offset = df.Offset - int64(e.Size())
					v.FileId = df.Id
					k.strIndex.skl.Insert(e.Meta.Key, v)
				}

			}
		}
	}

	// 删除旧文件
	for _, v := range k.archFiles {
		if err := os.Remove(v.File.Name()); err != nil {
			return err
		}
	}
	// 扶正临时区数据库文件
	for _, v := range newArchFiles {
		path := store.PathSeparator + fmt.Sprintf(store.DbFileNameFormat, v.Id)
		if err := os.Rename(rewrites+path, k.config.DirPath+path); err != nil {
			return err
		}
	}

	// 重写之后， 更新数据库信息
	if activeFileId == 0 {
		df, err = store.NewKvFile(k.config.DirPath, activeFileId, k.config.Method)
		if err != nil {
			return err
		}
	} else {
		activeFileId--
		df = newArchFiles[activeFileId]
		delete(newArchFiles, activeFileId)
	}
	// 修改归档文件指向
	k.archFiles = newArchFiles

	// 更新当前活跃文件
	k.activeFile = df
	k.activeFileId = activeFileId

	// 返回
	return nil
}

// 用来整理归档数据库文件， 删除冗余数据
func (k *Kvstore) validEntry(e *store.Entry) bool {
	if e == nil {
		return false
	}

	switch e.Type {
	case String:
		if e.Mark == StringSet {
			// 过期字典相关处理

			// 对比值是否一致
			if v, err := k.Get(e.Meta.Key); err == nil && string(e.Meta.Value) == string(v) {
				return true
			}
		}
	}
	// 返回
	return false
}
