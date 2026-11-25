package store

import (
	"encoding/json"
	"errors"
	"path/filepath"
	"testing"

	"github.com/killbane1232/huginn-messenger/internal/config"
)

func TestUpgradeRemovesLegacyTURNSettings(t *testing.T) {
	path := filepath.Join(t.TempDir(), "huginn.db")
	st, err := New(path)
	if err != nil {
		t.Fatal(err)
	}
	for _, key := range []string{"turn_addr", "turn_user", "turn_pass", "username"} {
		if err := st.SetConfigValue(key, "legacy"); err != nil {
			t.Fatal(err)
		}
	}
	if _, err := st.db.Exec(`DELETE FROM schema_version WHERE id = '013'`); err != nil {
		t.Fatal(err)
	}
	st.Close()
	st, err = New(path)
	if err != nil {
		t.Fatal(err)
	}
	defer st.Close()
	for _, key := range []string{"turn_addr", "turn_user", "turn_pass"} {
		if _, err := st.GetConfigValue(key); !errors.Is(err, ErrConfigNotFound) {
			t.Fatalf("legacy key %s remains: %v", key, err)
		}
	}
	if username, err := st.GetConfigValue("username"); err != nil || username != "legacy" {
		t.Fatalf("unrelated setting changed: %q, %v", username, err)
	}
	var cfg config.Config
	if err := json.Unmarshal([]byte(`{"muninn":"https://server.example","turn_addr":"legacy","turn_user":"legacy","turn_pass":"legacy"}`), &cfg); err != nil {
		t.Fatal(err)
	}
	if err := st.SaveAppConfig(&cfg); err != nil {
		t.Fatal(err)
	}
	for _, key := range []string{"turn_addr", "turn_user", "turn_pass"} {
		if _, err := st.GetConfigValue(key); !errors.Is(err, ErrConfigNotFound) {
			t.Fatalf("legacy import restored %s: %v", key, err)
		}
	}
}

func TestICEProfileMigrationOnlyInitializesKnownMuninn(t *testing.T) {
	for _, test := range []struct {
		name  string
		key   string
		value string
		want  bool
	}{
		{"selected default", ConfigMuninn, "https://muninn.evil-bread.ru", true},
		{"alternative default", ConfigMuninnServers, `["https://muninn.evil-bread.ru"]`, true},
		{"custom server", ConfigMuninn, "https://custom.example", false},
	} {
		t.Run(test.name, func(t *testing.T) {
			path := filepath.Join(t.TempDir(), "huginn.db")
			st, err := New(path)
			if err != nil {
				t.Fatal(err)
			}
			if err := st.SetConfigValue(test.key, test.value); err != nil {
				t.Fatal(err)
			}
			if _, err := st.db.Exec(`DELETE FROM schema_version WHERE id = '014'`); err != nil {
				t.Fatal(err)
			}
			st.Close()
			st, err = New(path)
			if err != nil {
				t.Fatal(err)
			}
			defer st.Close()
			cfg, err := st.LoadAppConfig()
			if err != nil {
				t.Fatal(err)
			}
			if !test.want && len(cfg.MuninnICE) != 0 {
				t.Fatal("migration assigned a public relay to a custom server")
			}
			if test.want && len(cfg.MuninnICE["https://muninn.evil-bread.ru"].TURNURLs) != 3 {
				t.Fatal("migration did not preserve the default server relay")
			}
		})
	}
}
