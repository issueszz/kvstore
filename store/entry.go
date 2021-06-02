package store

import (
	"encoding/binary"
	"errors"
	"hash/crc32"
)

var (
	ErrInvalidEntry = errors.New("invalid entry")
	ErrInValidCrc = errors.New("invalid crc")
)

const (
	entryHeaderSize = 20
)
type (
	Meta struct {
		Key []byte
		Value []byte
		Extra []byte
		KeySize uint32
		ValueSize uint32
		ExtraSize uint32
	}

	Entry struct {
		Meta *Meta
		Type uint16
		Mark uint16
		crc32 uint32
	}
)

// NewEntry 封装成entry对象
func NewEntry(key, value, extra []byte, t, mark uint16) *Entry {
	return &Entry{
		Meta: &Meta{
			Key: key,
			Value: value,
			Extra: extra,
			KeySize: uint32(len(key)),
			ValueSize: uint32(len(value)),
			ExtraSize: uint32(len(extra)),
		},
		Type: t,
		Mark: mark,
	}
}

// NewNoExtraEntry 封装成没有额外信息的对象
func NewNoExtraEntry(key, value []byte, t, mark uint16) *Entry{
	return NewEntry(key, value, nil, t, mark)
}

// Size 返回entry实例的大小
func (e *Entry) Size() uint32 {
	return entryHeaderSize+e.Meta.KeySize+e.Meta.ValueSize+e.Meta.ExtraSize
}

// Encode 返回entry编码结果
func (e *Entry) Encode() ([]byte, error) {
	if e == nil || e.Meta.KeySize == 0 {
		return nil, ErrInvalidEntry
	}
	buf := make([]byte, e.Size())
	ks, vs, es := e.Meta.KeySize, e.Meta.ValueSize, e.Meta.ExtraSize
	binary.BigEndian.PutUint32(buf[4:8], ks)
	binary.BigEndian.PutUint32(buf[8:12], vs)
	binary.BigEndian.PutUint32(buf[12:16], es)
	binary.BigEndian.PutUint16(buf[16:18], e.Type)
	binary.BigEndian.PutUint16(buf[18:20], e.Mark)

	copy(buf[entryHeaderSize:entryHeaderSize+ks], e.Meta.Key)
	copy(buf[entryHeaderSize+ks:entryHeaderSize+ks+vs], e.Meta.Value)
	if es > 0 {
		copy(buf[entryHeaderSize+ks+vs:entryHeaderSize+ks+vs+es], e.Meta.Extra)
	}

	// 校验和crc32
	crc := crc32.ChecksumIEEE(e.Meta.Value)
	binary.BigEndian.PutUint32(buf[:4], crc)
	return buf, nil
}

// Decode 返回解码得到的entry
func Decode(buf []byte) (*Entry, error) {
	crc := binary.BigEndian.Uint32(buf[:4])
	ks := binary.BigEndian.Uint32(buf[4:8])
	vs := binary.BigEndian.Uint32(buf[8:12])
	es := binary.BigEndian.Uint32(buf[12:16])
	t := binary.BigEndian.Uint16(buf[16:18])
	mark := binary.BigEndian.Uint16(buf[18:20])
	return &Entry{Meta: &Meta{KeySize: ks, ValueSize: vs, ExtraSize: es}, Type: t, Mark: mark, crc32: crc}, nil
}