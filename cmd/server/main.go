package main

import (
	"flag"
	"fmt"
	"github.com/pelletier/go-toml"
	"io/ioutil"
	"kvstore"
	"kvstore/cmd"
	"log"
	"os"
	"os/signal"
	"syscall"
)

// 预备工作
func init() {
	fmt.Println("just a test")
}

var configPath = flag.String("c", "", "config of kvstore")
//var path = flag.String("p", "", "dir path of kvstore")

func main()  {
	// 命令解析
	flag.Parse()
	var cfg *kvstore.Config
	// 读取配置文件
	if *configPath == "" {
		// 使用默认配置文件
		cfg = kvstore.DefaultConfig()
	} else {
		var err error
		cfg, err = loadConfig(*configPath)
		if err != nil {
			log.Printf(" load cofigure form file failed : %s", err.Error())
			return
		}
	}
	// 拉起服务
	ser, err := cmd.NewServer(cfg)
	if err != nil {
		log.Printf("server init failed : %s", err.Error())
	}

	// 监听中断事件
	sig := make(chan os.Signal, 1)
	signal.Notify(sig, os.Interrupt, os.Kill, syscall.SIGHUP,
		syscall.SIGINT, syscall.SIGTERM, syscall.SIGQUIT)

	// 使用另一个协程监听连接
	go ser.Listen(cfg.Addr)

	// 使用通道阻塞该协程
	<-sig
	//得到中断信号后， 关闭服务, 同时产生一个信号关闭监听协程
	ser.Close()
	log.Printf("quit kvstore, byte\n")
	// 中断事件到来使得该协程不再阻塞， 则关闭服务
}

// 加载配置文件
func loadConfig(path string) (*kvstore.Config, error) {
	buf, err := ioutil.ReadFile(path)
	if err != nil {
		return nil, err
	}
	var config *kvstore.Config
	err = toml.Unmarshal(buf, config)
	if err != nil {
		return nil, err
	}
	return config, nil
}
