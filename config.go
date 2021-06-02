package kvstore

import "kvstore/store"

type IndexDataMode int8
const (
	// KeyValueMode 键值均写入索引表模式
	KeyValueMode IndexDataMode = iota
	// OnlyKeyMode 只将键写入索引表模式
	OnlyKeyMode
)
const (
	// DefaultAddr 服务端地址
	DefaultAddr = "127.0.0.1:5000"

	// DefaultDirPath 数据库目录
	DefaultDirPath = "/tmp/kvStore/kvFiles"

	// DefaultMethod IO模式
	DefaultMethod = store.FileIO

	// DefaultBlockSize 默认每个数据文件的大小
	DefaultBlockSize = 16 * 1024

	// DefaultMaxKeySize  默认键的最大长度
	DefaultMaxKeySize = uint32(128)

	// DefaultMaxValueSize 默认值的最大长度
	DefaultMaxValueSize = uint32(1*1024)

	// DefaultReWriteThreshold 默认数据库文件重写阈值
	DefaultReWriteThreshold int = 4

)

type Config struct {
	Addr             string             `toml:"addr" json:"addr,omitempty"`
	DirPath          string             `toml:"dir_path" json:"dir_path,omitempty"`
	Method           store.FileRwMethod `toml:"method" json:"method,omitempty"`
	IdxMode          IndexDataMode      `toml:"idx_mode" json:"idx_mode,omitempty"`
	BlockSize        int64              `toml:"block_size" json:"block_size,omitempty"`
	Sync             bool               `toml:"sync" json:"sync,omitempty"`
	MaxKeySize       uint32             `toml:"max_key_size" json:"max_key_size,omitempty"`
	MaxValueSize     uint32             `toml:"max_value_size" json:"max_value_size,omitempty"`
	ReWriteThreshold int                `toml:"re_write_threshold" json:"re_write_threshold,omitempty"`
}

func DefaultConfig() *Config {
	return &Config{
		Addr: DefaultAddr,
		DirPath: DefaultDirPath,
		Method: DefaultMethod,
		IdxMode: KeyValueMode,
		BlockSize: DefaultBlockSize,
		Sync: false,
		MaxKeySize: DefaultMaxKeySize,
		MaxValueSize: DefaultMaxValueSize,
		ReWriteThreshold: DefaultReWriteThreshold,
	}
}
