package store

import (
	"crypto/sha256"
	"database/sql"
	"embed"
	"fmt"
	"io/fs"
	"log"
	"sort"
	"strings"
	"time"
)

//go:embed migrations/sqlite
var migrationsFS embed.FS

type Migration struct {
	ID          string
	Author      string
	Description string
	SQLite      string
}

var migrations []Migration

func init() {
	var err error
	migrations, err = loadMigrations()
	if err != nil {
		panic(fmt.Sprintf("load migrations: %v", err))
	}
}

func parseMigrationFile(name string) (id, desc string) {
	name = strings.TrimSuffix(name, ".sql")
	idx := strings.IndexByte(name, '_')
	if idx < 0 {
		return "", ""
	}
	id = name[:idx]
	desc = strings.ReplaceAll(name[idx+1:], "_", " ")
	return
}

func loadMigrations() ([]Migration, error) {
	entries, err := fs.ReadDir(migrationsFS, "migrations/sqlite")
	if err != nil {
		return nil, fmt.Errorf("read sqlite migrations: %w", err)
	}

	sort.Slice(entries, func(i, j int) bool {
		return entries[i].Name() < entries[j].Name()
	})

	var result []Migration
	for _, e := range entries {
		name := e.Name()
		id, desc := parseMigrationFile(name)
		if id == "" {
			log.Printf("migration: skip invalid filename %q", name)
			continue
		}

		sqliteSQL, err := migrationsFS.ReadFile("migrations/sqlite/" + name)
		if err != nil {
			return nil, fmt.Errorf("read sqlite/%s: %w", name, err)
		}

		result = append(result, Migration{
			ID:          id,
			Author:      "muninn",
			Description: desc,
			SQLite:      strings.TrimSpace(string(sqliteSQL)),
		})
	}

	return result, nil
}

func checksum(m Migration) string {
	h := sha256.Sum256([]byte(m.SQLite))
	return fmt.Sprintf("%x", h[:8])
}

func schemaVersionTable() string {
	return `CREATE TABLE IF NOT EXISTS schema_version (
		id TEXT PRIMARY KEY,
		author TEXT NOT NULL,
		description TEXT NOT NULL DEFAULT '',
		applied_at INTEGER NOT NULL,
		checksum TEXT NOT NULL DEFAULT ''
	)`
}

func runMigrations(db *sql.DB) error {
	ddl := schemaVersionTable()

	if _, err := db.Exec(ddl); err != nil {
		return fmt.Errorf("create schema_version: %w", err)
	}

	applied := make(map[string]bool)
	rows, err := db.Query(`SELECT id FROM schema_version`)
	if err == nil {
		defer rows.Close()
		for rows.Next() {
			var id string
			if err := rows.Scan(&id); err == nil {
				applied[id] = true
			}
		}
	}

	for _, m := range migrations {
		if applied[m.ID] {
			continue
		}

		var sql = m.SQLite

		if sql == "" {
			continue
		}

		log.Printf("Migration %s: %s", m.ID, m.Description)

		if _, err := db.Exec(sql); err != nil {
			if strings.Contains(err.Error(), "duplicate column name") {
				log.Printf("Migration %s: column already exists, skipping", m.ID)
			} else {
				return fmt.Errorf("migration %s: %w", m.ID, err)
			}
		}

		cs := checksum(m)
		now := time.Now().Unix()
		if _, err := db.Exec(
			`INSERT INTO schema_version (id, author, description, applied_at, checksum) VALUES ($1, $2, $3, $4, $5)`,
			m.ID, m.Author, m.Description, now, cs,
		); err != nil {
			return fmt.Errorf("record migration %s: %w", m.ID, err)
		}
	}

	return nil
}
