package main

import (
	"context"
	"embed"
	"errors"
	"log"
	"log/slog"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"

	_ "modernc.org/sqlite"
)

const ServerAddress = "127.0.0.1:6401"

func main() {
	setupLog()
	logOntoDB()
	serverSetup()
	serverHandling()
}

func setupLog() {
	log.SetOutput(os.Stdout)
	log.SetFlags(log.LstdFlags | log.Llongfile)
	slog.SetLogLoggerLevel(slog.LevelInfo)
}

//go:embed public/*
var publicFiles embed.FS

func serverSetup() {
	fs := http.FileServerFS(publicFiles)
	http.Handle("GET /public/", fs)

	setupPages()
	http.HandleFunc("GET /", getStartPage)
}

func serverHandling() {
	log.Println("Starting HTML Server: Use http://" + ServerAddress)
	server := &http.Server{
		Addr: ServerAddress,
	}

	go func() {
		if err := server.ListenAndServe(); !errors.Is(err, http.ErrServerClosed) {
			log.Fatalf("HTTP server error: %v", err)
		}
		log.Println("Stopped serving new connections.")
	}()

	sigChan := make(chan os.Signal, 1)
	signal.Notify(sigChan, syscall.SIGINT, syscall.SIGTERM)
	<-sigChan

	shutdownCtx, shutdownRelease := context.WithTimeout(context.Background(), 10*time.Second)
	defer shutdownRelease()

	if err := server.Shutdown(shutdownCtx); err != nil {
		log.Fatalf("HTTP shutdown error: %v", err)
	}
	closeDB()
	log.Println("Graceful shutdown complete.")
}
