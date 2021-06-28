package main

import (
	"bufio"
	"encoding/binary"
	"flag"
	"fmt"
	"github.com/peterh/liner"
	"log"
	"net"
	"os"
	"path/filepath"
	"strings"
)

var host = flag.String("p", "127.0.0.1", "input host to connect, default 127.0.0.1")
var port = flag.String("h", "5000", "input port to connect, default 5000")

var (
	commandLists = [][]string{
		{"set", "key value", "string"},
		{"get", "key", "string"},
		{"expire", "key value", "string"},
	}
	historyFn = filepath.Join(os.TempDir(), ".liner_example_history")
)


func main() {
	addr := fmt.Sprintf("%s:%s", *host, *port)
	conn, err := net.Dial("tcp", addr)
	if err != nil {
		log.Printf("connect kvstore failed : %s\n", err.Error())
	}
	defer conn.Close()

	line := liner.NewLiner()
	defer line.Close()

	line.SetCtrlCAborts(true)
	// 添加补全命令
	line.SetCompleter(func(line string) (c []string) {
		for _, cmd := range commandLists {
			if strings.HasPrefix(cmd[0], strings.ToLower(line)) {
				c = append(c, cmd[0])
			}
		}
		return
	})

	// 打开历史命令
	if f, err := os.Open(historyFn); err == nil {
		line.ReadHistory(f)
		f.Close()
	}

	// 将命令添加到集合中， 便于查找
	commandSet := make(map[string]bool)
	for _, cmd := range commandLists {
		commandSet[strings.ToLower(cmd[0])] = true
	}
	// 命令提示符前缀
	prompt := addr + ">"
	for {
		cmd, err := line.Prompt(prompt)
		if err != nil {
			fmt.Printf("Error reading line : %+v\n", err)
			break
		}

		// 处理多余空白字符
		cmd = strings.TrimSpace(cmd)
		if len(cmd) == 0 {
			continue
		}

		// 全部转化为小写字符
		cmd = strings.ToLower(cmd)

		// 按空白符号分割字符串， 得到命令及命令所需要的数据
		cmdAndArgs := strings.Split(cmd, " ")

		if cmd == "quit" {
			break
		} else if cmd == "help" {
			printCmdHelp()
		} else if len(cmdAndArgs) == 2 && cmdAndArgs[0] == "help" {
			if !commandSet[cmdAndArgs[1]] {
				fmt.Printf("command not found\n")
				continue
			}
			for _, cmd := range commandLists {
				if cmd[0] ==  cmdAndArgs[1] {
					fmt.Println("--usage: ", cmd[0], cmd[1])
					fmt.Println("--group: ", cmd[2])
				}
			}
		} else {
			// 添加该命令到历史命令
			line.AppendHistory(cmd)

			// 判断命令是否存在
			if !commandSet[cmdAndArgs[0]] {
				fmt.Println("command not found")
				continue
			}

			// 封装命令， 传递给服务端
			_, err := conn.Write(wrapCmd(cmd))
			if err != nil {
				fmt.Println(err)
			}
			// 接受服务端返回的数据
			reply := readReply(conn)
			fmt.Println(reply)
		}
	}

	// 历史命令写入文件
	if f, err := os.Create(historyFn); err != nil {
		fmt.Printf("Error write history file : %+v\n", err)
	} else {
		line.WriteHistory(f)
		f.Close()
	}
}

func wrapCmd(args string) []byte {
	data := make([]byte, len(args)+4)
	binary.BigEndian.PutUint32(data[:4], uint32(len(args)))
	copy(data[4:], args)
	return data
}

func readReply(conn net.Conn) (res string) {
	bufReader := bufio.NewReader(conn)
	buf := make([]byte, 4)
	_, err := bufReader.Read(buf)
	if err != nil {
		return
	}
	size := binary.BigEndian.Uint32(buf)
	if size > 0 {
		data := make([]byte, size)
		_, err = bufReader.Read(data)
		if err == nil {
			res = string(data)
		}
	}
	return
}

func printCmdHelp() {
help :=`To get help about command:
	Type: "help <command>" for help on command
To quit:
	<ctrl+c> or <quit>`
	fmt.Println(help)
}