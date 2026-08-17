package main

import (
	"log"
	"os"
	"os/signal"
	"syscall"

	"github.com/killbane1232/huginn-messenger/internal/config"
	"github.com/killbane1232/huginn-messenger/internal/messenger"
	"github.com/killbane1232/huginn-messenger/internal/muninn"
)

func main() {
	cfg := config.Parse()
	if cfg == nil {
		os.Exit(1)
	}

	mc := muninn.NewClient(cfg.MuninnAddr)
	opts := []messenger.MessengerOption{messenger.WithPeerFlag(muninn.PeerFlag(cfg.PeerFlag))}
	if cfg.PeerID != "" {
		opts = append(opts, messenger.WithPeerID(cfg.PeerID))
	}
	m, err := messenger.New(cfg.Username, mc, cfg.DBPath, opts...)
	if err != nil {
		log.Fatalf("failed to create messenger: %v", err)
	}

	if err := m.Register(); err != nil {
		log.Printf("warning: register failed: %v", err)
	}

	log.Printf("started: username=%s muninn=%s db=%s", cfg.Username, cfg.MuninnAddr, cfg.DBPath)

	sigCh := make(chan os.Signal, 1)
	signal.Notify(sigCh, syscall.SIGINT, syscall.SIGTERM)
	<-sigCh

	log.Println("shutting down...")
	m.Shutdown()
}
