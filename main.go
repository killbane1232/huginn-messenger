package main

import (
	"fmt"
	"log"
	"os"
	"os/signal"
	"syscall"

	"github.com/killbane1232/huginn-messenger/internal/config"
	"github.com/killbane1232/huginn-messenger/internal/messenger"
	"github.com/killbane1232/huginn-messenger/internal/muninn"
	"github.com/killbane1232/huginn-messenger/internal/ui"
)

func main() {
	cfg := config.Parse()
	if cfg == nil {
		os.Exit(1)
	}

	mc := muninn.NewClient(cfg.MuninnAddr)

	m, err := messenger.New(cfg.Username, mc, cfg.DBPath)
	if err != nil {
		log.Fatalf("failed to create messenger: %v", err)
	}

	if err := m.Register(); err != nil {
		log.Printf("warning: register failed: %v", err)
	}

	uiSrv := ui.NewServer(cfg.UIPort, m)

	go func() {
		log.Printf("web UI started at http://localhost:%d", cfg.UIPort)
		log.Printf("connect to muninn: %s", cfg.MuninnAddr)
		if err := uiSrv.Start(); err != nil {
			log.Fatalf("UI server error: %v", err)
		}
	}()

	fmt.Printf("\n  Open http://localhost:%d in your browser\n\n", cfg.UIPort)

	sigCh := make(chan os.Signal, 1)
	signal.Notify(sigCh, syscall.SIGINT, syscall.SIGTERM)
	<-sigCh

	log.Println("shutting down...")
	uiSrv.Shutdown()
	m.Shutdown()
}
