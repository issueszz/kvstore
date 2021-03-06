package cmd

import (
	"errors"
	"kvstore"
	"strconv"
)

var ErrSyntax = errors.New("invalid syntax")
func set(kv *kvstore.Kvstore, args []string) (res string, err error) {
	//检查参数是否合格
	if len(args) != 2 {
		err = ErrSyntax
		return
	}
	if err = kv.Set([]byte(args[0]), []byte(args[1])); err == nil {
		res = "ok"
	}
	return
}

func get(kv *kvstore.Kvstore, args []string) (res string, err error) {
	// 检查参数
	if len(args) != 1 {
		err = ErrSyntax
		return
	}
	var data []byte
	data, err = kv.Get([]byte(args[0]))
	res = string(data)
	return
}

func expire(kv *kvstore.Kvstore, args []string) (res string, err error) {
	if len(args) != 2 {
		err = ErrSyntax
		return
	}
	seconds, err := strconv.Atoi(args[1])
	if err != nil {
		err = ErrSyntax
		return
	}
	if err = kv.Expire([]byte(args[0]), uint64(seconds)); err == nil {
		res = "OK"
	}
	return
}
func init() {
	addCmdHandle("set", set)
	addCmdHandle("get", get)
	addCmdHandle("expire", expire)
}
