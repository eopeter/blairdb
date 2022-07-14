package main

import (
	"context"
	"github.com/eopeter/blairdb/src"
	"log"
	"os"
	"os/signal"
	"syscall"
	"time"
)

func main() {
	db := database.New(1, 3, 2)
	err := db.Write([]byte("front"), []byte("world"))
	if err != nil {
		log.Fatal(err)
	}
	done := make(chan os.Signal, 1)
	signal.Notify(done, os.Interrupt, syscall.SIGINT, syscall.SIGTERM)
	log.Println("Database Started")
	<-done
	log.Println("Database Stopped")
	_, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer func() {
		db.Close()
		cancel()
	}()
	log.Println("Database Exited Properly")
}
