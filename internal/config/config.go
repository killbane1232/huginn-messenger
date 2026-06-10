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
	DBPath     string
}

func Parse() *Config {
	c := &Config{}
	flag.StringVar(&c.MuninnAddr, "muninn", "http://localhost:8080", "muninn server address")
	flag.StringVar(&c.Username, "username", "", "your username (required)")
	flag.IntVar(&c.UIPort, "ui-port", 0, "web UI port (default: random)")
	flag.StringVar(&c.DBPath, "db", "huginn.db", "path to SQLite database")
	flag.Parse()

	if c.Username == "" {
		fmt.Println("Error: --username is required")
		flag.Usage()
		return nil
	}
	if c.UIPort == 0 {
		c.UIPort = findFreePort()
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
