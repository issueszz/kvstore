package store

import (
	"errors"
	"fmt"
	"github.com/roseduan/mmap-go"
	"hash/crc32"
	"io/ioutil"
	"os"
	"sort"
	"strconv"
	"strings"
)

var (
	ErrEmptyEntry = errors.New("entry or the key of entry is empty")
)

const (
	// FilePerm 默认创建文件属性
	FilePerm = 0644

	// DbFileNameFormat 默认文件名称格式
	DbFileNameFormat = "%06d.data"

	// PathSeparator 路径连接符
	PathSeparator = string(os.PathSeparator)
)

type FileRwMethod uint8
const (
	FileIO FileRwMethod = iota
	// MmapIO 模式
	MmapIO
)

type KvFile struct {
	Id     uint32 // 数据库文件id
	path   string // 文件路径
	method FileRwMethod //读写文件方式
	File   *os.File //普通读写文件方式
	Offset int64 // 偏移地址
	Mp mmap.MMap // 增加了mmap模式
}

// NewKvFile 创建一个新的数据库文件
func NewKvFile(path string, fid uint32, method FileRwMethod, blockSize int64) (*KvFile, error) {
	filepath := path + PathSeparator + fmt.Sprintf(DbFileNameFormat, fid)

	f, err := os.OpenFile(filepath, os.O_CREATE|os.O_RDWR, FilePerm)
	if err != nil {
		return nil, err
	}

	kf := &KvFile{Id: fid, path: path, method: method, Offset: 0}

	if kf.method == FileIO {
		kf.File = f
	} else {
		if err := f.Truncate(blockSize); err != nil {
			return nil, err
		}
		m, err := mmap.Map(f, os.O_RDWR, 0)
		if err != nil {
			return nil, err
		}
		kf.Mp = m
	}

	// 返回
	return kf, nil
}

// 数据库文件读操作
func (kf *KvFile) Read(offset int64) (e *Entry, err error) {
	var buf []byte
	// 解码entry， 返回一个只包含head的entry
	buf, err = kf.rebuff(offset, int64(entryHeaderSize))
	if err != nil {
		return
	}
	e, err = Decode(buf)
	if err != nil {
		return
	}

	offset += entryHeaderSize
	// 解码Meta中的key
	if e.Meta.KeySize > 0 {
		var key []byte
		if key, err = kf.rebuff(offset, int64(e.Meta.KeySize)); err != nil {
			return
		}
		e.Meta.Key = key
	}

	offset += int64(e.Meta.KeySize)
	// 解码Meta中的Value
	if e.Meta.ValueSize > 0 {
		var value []byte
		if value, err = kf.rebuff(offset, int64(e.Meta.ValueSize)); err != nil {
			return
		}
		e.Meta.Value = value
	}

	offset += int64(e.Meta.ValueSize)
	// 解码Meta中的额外数据
	if e.Meta.ExtraSize > 0 {
		var extra []byte
		if extra, err = kf.rebuff(offset, int64(e.Meta.ExtraSize)); err != nil {
			return
		}
		e.Meta.Extra = extra
	}

	// 检查结果
	checkCrc32 := crc32.ChecksumIEEE(e.Meta.Value)
	if checkCrc32 != e.crc32 {
		return nil, ErrInValidCrc
	}

	//返回结果
	return e, err
}

func (kf *KvFile) rebuff(offset int64, n int64) ([]byte, error) {
	buf := make([]byte, n)

	if kf.method == FileIO {
		_, err := kf.File.ReadAt(buf, offset)
		if err != nil {
			return nil, err
		}
	}
	if kf.method == MmapIO && offset <= int64(len(kf.Mp)) {
		copy(buf, kf.Mp[offset:])
	}
	// 返回结果
	return buf, nil
}

// 从文件偏移处写入数据
func (kf *KvFile) Write(e *Entry) error {
	if e == nil || e.Meta.KeySize == 0 {
		return ErrEmptyEntry
	}
	buf, err := e.Encode()
	if err != nil {
		return err
	}
	if kf.method == FileIO {
		_, err := kf.File.WriteAt(buf, kf.Offset)
		if err != nil {
			return err
		}
	}

	if kf.method == MmapIO {
		copy(kf.Mp[kf.Offset:], buf)
	}

	kf.Offset += int64(e.Size())

	return nil
}

// Sync 同步文件
func (kf * KvFile) Sync() (err error) {
	if kf.File != nil {
		err = kf.File.Sync()
	}
	if kf.Mp != nil {
		err = kf.Mp.Unmap()
	}
	return
}

// Close 关闭当前文件
func (kf *KvFile) Close(sync bool) (err error) {
	// 根据配置文件决定是否关闭文件前进行同步
	if sync {
		if err = kf.Sync(); err != nil {
			return
		}
	}

	if kf.File != nil {
		err = kf.File.Close()
	}

	if kf.Mp != nil {
		err = kf.Mp.Flush()
	}
	return
}

// Build 加载数据文件信息
func Build(path string, method FileRwMethod, blockSize int64) (map[uint32]*KvFile, uint32, error)  {
	dir, err := ioutil.ReadDir(path)
	if err != nil {
		return nil, 0, nil
	}

	var activeFileId uint32 = 0
	var FileIds []int
	for _, d := range dir {
		if strings.HasSuffix(d.Name(), "data") {
			splitName := strings.Split(d.Name(), ".")
			id, _ := strconv.Atoi(splitName[0])
			FileIds = append(FileIds, id)
		}
	}

	sort.Ints(FileIds)
	archFiles := make(map[uint32]*KvFile)
	if len(FileIds) > 0 {
		activeFileId = uint32(FileIds[len(FileIds)-1])
		// 文件序号递增， 最大序号的文件如果未填满则设置为当前活跃文件
		for i := 0; i < len(FileIds)-1; i++ {
			id := uint32(FileIds[i])
			file, err := NewKvFile(path, id, method, blockSize)
			if err != nil {
				return nil, activeFileId, nil
			}
			archFiles[id] = file
		}
	}

	// 返回
	return archFiles, activeFileId, nil
}




