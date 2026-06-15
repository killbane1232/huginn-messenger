package config

import (
	"encoding/json"
	"flag"
	"fmt"
	"net"
	"os"
	"strings"
)

type Config struct {
	MuninnAddr string `json:"muninn"`
	Username   string `json:"username"`
	UIPort     int    `json:"ui_port"`
	DBPath     string `json:"-"`
	ChunkTTL   string `json:"chunk_ttl"`
	PeerFlag   string `json:"peer_flag"`
}

const configPath = "config.conf"

func Parse() *Config {
	c := &Config{
		MuninnAddr: "http://localhost:8080",
		DBPath:     "huginn.db",
		ChunkTTL:   "1w",
		PeerFlag:   "thin",
	}

	if data, err := os.ReadFile(configPath); err == nil {
		if err := json.Unmarshal(data, c); err != nil {
			fmt.Fprintf(os.Stderr, "warning: failed to parse %s: %v\n", configPath, err)
		}
	}

	flag.StringVar(&c.MuninnAddr, "muninn", c.MuninnAddr, "muninn server address")
	flag.StringVar(&c.Username, "username", c.Username, "your username (required)")
	flag.IntVar(&c.UIPort, "ui-port", c.UIPort, "web UI port (default: random)")
	flag.StringVar(&c.DBPath, "db", c.DBPath, "path to SQLite database")
	flag.StringVar(&c.ChunkTTL, "chunk-ttl", c.ChunkTTL, "chunk TTL (1d, 1w, 1m)")
	flag.StringVar(&c.PeerFlag, "peer-flag", c.PeerFlag, "peer flag: thin, thick, very_thick")
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

func ChunkTTLSeconds(ttl string) int {
	switch strings.ToLower(ttl) {
	case "1d":
		return 86400
	case "1w":
		return 604800
	case "1m":
		return 2592000
	default:
		return 604800
	}
}

func (c *Config) Save() error {
	data, err := json.MarshalIndent(c, "", "  ")
	if err != nil {
		return err
	}
	return os.WriteFile(configPath, data, 0644)
}

func findFreePort() int {
	l, err := net.Listen("tcp", ":0")
	if err != nil {
		return 0
	}
	defer l.Close()
	return l.Addr().(*net.TCPAddr).Port
}
