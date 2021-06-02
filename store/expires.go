package store

import (
	"encoding/binary"
	"io"
	"os"
)

const expiresHeadSize = 12

type Expires map[string]uint64

type element struct {
	Key []byte
	KeySize uint32
	Deadline uint64
}

// SaveExpires 保存过期字典
func (e Expires) SaveExpires(path string) error {
	// 打开给定路径的文件
	file, err := os.OpenFile(path, os.O_CREATE|os.O_RDWR|os.O_TRUNC, FilePerm)
	if err != nil {
		return err
	}
	defer file.Close()

	// 偏移量
	var offset int64 = 0

	// 遍历过期字典， 对于每条数据按一定的格式写入文件
	for k, v := range e {
		key := []byte(k)
		deadline := v
		buf := make([]byte, len(key)+expiresHeadSize)
		// 前12字节写入数据头
		binary.BigEndian.PutUint32(buf[:4], uint32(len(key)))
		binary.BigEndian.PutUint64(buf[4:12], deadline)
		copy(buf[12:], key)

		// 将该条数据写入文件
		_, err = file.WriteAt(buf, offset)
		if err != nil {
			return err
		}

		// 更新偏移地址
		offset += int64(expiresHeadSize+len(key))
	}

	// 返回
	return nil
}

// LoadExpires 加载过期字典
func LoadExpires(path string) (Expires, error) {
	// 按照给定路径打开文件
	file, err := os.OpenFile(path, os.O_CREATE|os.O_RDWR, FilePerm)
	if err != nil {
		return nil, err
	}
	defer file.Close()

	expires := make(Expires)
	var offset int64 = 0

	// 读取数据文件直到文件末尾或者错误发生
	for {
		if ele, err := readExpires(file, offset); err == nil {
			key := string(ele.Key)
			expires[key] = ele.Deadline
			offset += int64(ele.KeySize + expiresHeadSize)
		} else {
			if err == io.EOF {
				break
			}
			return nil, err
		}
	}

	// 返回
	return expires, nil
}

// 读取文件并解码
func readExpires(file *os.File, offset int64) (*element, error) {
	buf := make([]byte, expiresHeadSize)
	_, err := file.ReadAt(buf, offset)
	if err != nil {
		return nil, err
	}

	ele := &element{}
	ele.KeySize = binary.BigEndian.Uint32(buf[:4])
	ele.Deadline = binary.BigEndian.Uint64(buf[4:12])

	key := make([]byte, ele.KeySize)
	_, err = file.ReadAt(key, offset+expiresHeadSize)
	if err != nil {
		return nil, err
	}
	ele.Key = key
	return ele, err
}






