package config

import (
	"encoding/json"
	"flag"
	"fmt"
	"os"
	"strings"

	"github.com/google/uuid"
)

type Config struct {
	MuninnServers []string `json:"muninn_servers,omitempty"`
	MuninnAddr    string   `json:"muninn"`
	Username      string   `json:"username"`
	PeerID        string   `json:"peer_id,omitempty"`
	DBPath        string   `json:"-"`
	ChunkTTL      string   `json:"chunk_ttl"`
	PeerFlag      string   `json:"peer_flag"`
	TurnAddr      string   `json:"turn_addr"`
	TurnUsername  string   `json:"turn_user"`
	TurnPassword  string   `json:"turn_pass"`
}

const configPath = "config.conf"

func Parse() *Config {
	c := &Config{
		DBPath:   "huginn.db",
		ChunkTTL: "1w",
		PeerFlag: "thin",
	}

	if data, err := os.ReadFile(configPath); err == nil {
		if err := json.Unmarshal(data, c); err != nil {
			fmt.Fprintf(os.Stderr, "warning: failed to parse %s: %v\n", configPath, err)
		}
	}
	if addr := strings.TrimSpace(os.Getenv("MUNINN_ADDR")); addr != "" {
		c.MuninnAddr = addr
	}

	flag.StringVar(&c.MuninnAddr, "muninn", c.MuninnAddr, "muninn server address")
	flag.StringVar(&c.Username, "username", c.Username, "your username (required)")
	flag.StringVar(&c.DBPath, "db", c.DBPath, "path to SQLite database")
	flag.StringVar(&c.ChunkTTL, "chunk-ttl", c.ChunkTTL, "chunk TTL (1d, 1w, 1m)")
	flag.StringVar(&c.PeerFlag, "peer-flag", c.PeerFlag, "peer flag: thin, thick, very_thick")
	known := strings.Join(c.MuninnServers, ",")
	if v, ok := os.LookupEnv("MUNINN_SERVERS"); ok {
		known = v
	}
	flag.StringVar(&known, "muninn-servers", known, "comma-separated Muninn mesh discovery hints")
	flag.Parse()
	c.MuninnServers = nil
	for _, address := range strings.Split(known, ",") {
		if address = strings.TrimSpace(address); address != "" {
			c.MuninnServers = append(c.MuninnServers, address)
		}
	}

	if c.Username == "" {
		c.Username = uuid.New().String()
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
