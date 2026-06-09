package config

import (
	"flag"
	"fmt"
	"net"
)

type Config struct {
	MuninnAddr string
	Username   string
	UIPort     int
	MsgPort    int
}

func Parse() *Config {
	c := &Config{}
	flag.StringVar(&c.MuninnAddr, "muninn", "http://localhost:8080", "muninn server address")
	flag.StringVar(&c.Username, "username", "", "your username (required)")
	flag.IntVar(&c.UIPort, "ui-port", 0, "web UI port (default: random)")
	flag.IntVar(&c.MsgPort, "msg-port", 0, "P2P message port (default: random)")
	flag.Parse()

	if c.Username == "" {
		fmt.Println("Error: --username is required")
		flag.Usage()
		return nil
	}
	if c.UIPort == 0 {
		c.UIPort = findFreePort()
	}
	if c.MsgPort == 0 {
		c.MsgPort = findFreePort()
	}
	return c
}

func findFreePort() int {
	l, _ := net.Listen("tcp", ":0")
	if l == nil {
		return 0
	}
	defer l.Close()
	return l.Addr().(*net.TCPAddr).Port
}
