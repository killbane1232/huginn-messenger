package store

import (
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"

	"github.com/killbane1232/huginn-messenger/internal/config"
)

const (
	ConfigCryptoKeys    = "crypto_keys"
	ConfigUsername      = "username"
	ConfigMuninn        = "muninn"
	ConfigMuninnServers = "muninn_servers"
	ConfigMuninnICE     = "muninn_ice"
	ConfigPeerID        = "peer_id"
	ConfigChunkTTL      = "chunk_ttl"
	ConfigPeerFlag      = "peer_flag"
)

var ErrConfigNotFound = errors.New("config value not found")

func (s *SQLiteStore) GetConfigValue(name string) (string, error) {
	var value string
	s.mu.Lock()
	defer s.mu.Unlock()
	err := s.db.QueryRow(`SELECT value FROM config_values WHERE name = ?`, name).Scan(&value)
	if errors.Is(err, sql.ErrNoRows) {
		return "", ErrConfigNotFound
	}
	return value, err
}

func (s *SQLiteStore) SetConfigValue(name, value string) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	_, err := s.db.Exec(
		`INSERT INTO config_values (name, value) VALUES (?, ?)
			ON CONFLICT(name) DO UPDATE SET value = excluded.value`,
		name, value,
	)
	return err
}

func (s *SQLiteStore) GetKeysJSON() (string, error) {
	return s.GetConfigValue(ConfigCryptoKeys)
}

func (s *SQLiteStore) SaveKeysJSON(data string) error {
	return s.SetConfigValue(ConfigCryptoKeys, data)
}

func (s *SQLiteStore) LoadAppConfig() (*config.Config, error) {
	cfg := &config.Config{}
	hasValue := false

	if v, err := s.GetConfigValue(ConfigUsername); err == nil {
		cfg.Username = v
		hasValue = true
	}
	if v, err := s.GetConfigValue(ConfigMuninn); err == nil {
		cfg.MuninnAddr = v
		hasValue = true
	}
	if v, err := s.GetConfigValue(ConfigMuninnServers); err == nil {
		if err := json.Unmarshal([]byte(v), &cfg.MuninnServers); err != nil {
			return nil, fmt.Errorf("parse muninn_servers: %w", err)
		}
		hasValue = true
	}
	if v, err := s.GetConfigValue(ConfigPeerID); err == nil {
		cfg.PeerID = v
		hasValue = true
	}
	if v, err := s.GetConfigValue(ConfigMuninnICE); err == nil {
		if err := json.Unmarshal([]byte(v), &cfg.MuninnICE); err != nil {
			return nil, fmt.Errorf("parse muninn_ice: %w", err)
		}
		hasValue = true
	}
	if v, err := s.GetConfigValue(ConfigChunkTTL); err == nil {
		cfg.ChunkTTL = v
		hasValue = true
	}
	if v, err := s.GetConfigValue(ConfigPeerFlag); err == nil {
		cfg.PeerFlag = v
		hasValue = true
	}
	if !hasValue {
		return nil, ErrConfigNotFound
	}
	return cfg, nil
}

func (s *SQLiteStore) SaveAppConfig(cfg *config.Config) error {
	if cfg == nil {
		return fmt.Errorf("config is nil")
	}
	if cfg.Username != "" {
		if err := s.SetConfigValue(ConfigUsername, cfg.Username); err != nil {
			return err
		}
	}
	if cfg.MuninnAddr != "" {
		if err := s.SetConfigValue(ConfigMuninn, cfg.MuninnAddr); err != nil {
			return err
		}
	}
	servers, err := json.Marshal(cfg.MuninnServers)
	if err != nil {
		return err
	}
	if err := s.SetConfigValue(ConfigMuninnServers, string(servers)); err != nil {
		return err
	}
	profiles, err := json.Marshal(cfg.MuninnICE)
	if err != nil {
		return err
	}
	if err := s.SetConfigValue(ConfigMuninnICE, string(profiles)); err != nil {
		return err
	}
	if cfg.PeerID != "" {
		if err := s.SetConfigValue(ConfigPeerID, cfg.PeerID); err != nil {
			return err
		}
	}
	if cfg.ChunkTTL != "" {
		if err := s.SetConfigValue(ConfigChunkTTL, cfg.ChunkTTL); err != nil {
			return err
		}
	}
	if cfg.PeerFlag != "" {
		if err := s.SetConfigValue(ConfigPeerFlag, cfg.PeerFlag); err != nil {
			return err
		}
	}
	return nil
}

func (s *SQLiteStore) ImportLegacyFiles(dbDir string) error {
	if _, err := s.GetConfigValue(ConfigCryptoKeys); errors.Is(err, ErrConfigNotFound) {
		keysPath := filepath.Join(dbDir, "keys.conf")
		if data, err := os.ReadFile(keysPath); err == nil {
			if err := s.SaveKeysJSON(string(data)); err != nil {
				return fmt.Errorf("import keys.conf: %w", err)
			}
		}
	}

	if _, err := s.LoadAppConfig(); errors.Is(err, ErrConfigNotFound) {
		configPath := filepath.Join(dbDir, "config.conf")
		if data, err := os.ReadFile(configPath); err == nil {
			var cfg config.Config
			if err := json.Unmarshal(data, &cfg); err != nil {
				return fmt.Errorf("parse config.conf: %w", err)
			}
			if err := s.SaveAppConfig(&cfg); err != nil {
				return fmt.Errorf("import config.conf: %w", err)
			}
		}
	}
	return nil
}
