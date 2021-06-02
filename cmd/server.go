package cmd

import (
	"bufio"
	"encoding/binary"
	"fmt"
	"kvstore"
	"log"
	"net"
	"regexp"
	"sync"
	"time"
)

var reg, _ = regexp.Compile(`'.*?'|".*?"|\S+`)
type handleFunc  = func(*kvstore.Kvstore, []string) (string, error)
var Handles = make(map[string]handleFunc)

// 添加命令处理函数
func addCmdHandle(cmd string, handle handleFunc) {
	Handles[cmd] = handle
}

const connInterval = 8
type server struct {
	kv *kvstore.Kvstore
	closed bool
	mu sync.Mutex
	done chan struct{}
	listener net.Listener
}

// NewServer 返回一个数据库服务器
func NewServer(config *kvstore.Config) (*server, error) {
	kv, err := kvstore.Open(config)
	return &server{kv: kv, done: make(chan struct{})}, err
}

func (s *server) Close() {
	if s.closed {
		return
	}
	// 上锁
	s.mu.Lock()
	defer s.mu.Unlock()

	// 关闭服务端口
	err := s.listener.Close()
	if err != nil {
		log.Printf("close listener %s\n", err.Error())
	}

	// 关闭数据库
	err = s.kv.Close()
	if err != nil {
		log.Printf("close kvstore %s\n", err.Error())
	}

	s.closed = true
	// 同步监听协程
	close(s.done)
}

func (s *server) Listen(addr string) {
	var err error
	// 初始化监听端口
	s.listener, err = net.Listen("tcp", addr)
	if err != nil {
		log.Printf("tcp listen failed :%v\n", err.Error())
		return
	}
	// 监听连接并且启用一个新的协程处理命令
	for {
		select {
		// 结束信号到来则结束该协程， 同步作用
		case <-s.done:
			return
		default:
			// 连接到来
			conn, err := s.listener.Accept()
			if err != nil {
				continue
			}
			// 启用新的协程处理请求
			go s.handleConn(conn)
		}
	}
}

func (s *server) handleConn(conn net.Conn) {
	defer conn.Close()
	for {
		// 设置过期时间
		_ = conn.SetReadDeadline(time.Now().Add(time.Hour * connInterval))

		// 包装下conn
		connReader := bufio.NewReader(conn)

		// 读取长度
		buf := make([]byte, 4)
		_, err := connReader.Read(buf)
		if err != nil {
			// 写入日志
			log.Printf("read cmd size err : %+v\n", err)
			break
		}

		size := binary.BigEndian.Uint32(buf)

		if size > 0 {
			// 根据长度读取命令和数据
			data := make([]byte, size)
			_, err = connReader.Read(data)
			if err != nil {
				log.Printf("read cmd data err : %+v\n", err)
				break
			}
			// 解码数据
			cmdAndArgs := reg.FindAllString(string(data), -1)
			// 执行命令
			info := s.handleCmd(cmdAndArgs[0], cmdAndArgs[1:])
			// 包装回复
			reply := wrapReplyInfo(info)
			// 回复客户端
			if _, err := conn.Write(reply); err != nil {
				log.Printf("write reply err %+v\n", err)
			}

		}

	}
}

// 执行命令统一接口
func (s *server) handleCmd(cmd string, args []string) string {
	defer func() {
		if r := recover(); r != nil {
			log.Printf("panic when executed cmd : %+v\n", r)
		}
	}()

	// 查看命令是否存在
	handle, exist := Handles[cmd]
	if !exist {
		return "cmd not exist"
	}
	// 执行命令
	ret, err := handle(s.kv, args)
	if err != nil {
		return fmt.Sprintf("err : %s", err.Error())
	}
	fmt.Println(ret)
	return ret
}


func wrapReplyInfo(info string) []byte {
	reply := make([]byte, 4+len(info))
	binary.BigEndian.PutUint32(reply[:4], uint32(len(info)))
	copy(reply[4:], info)
	return reply
}




