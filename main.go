package main

import (
	"context"
	"flag"
	"log"
	"os"
	"os/signal"
	"sync"
	"syscall"
)

func main() {
	configPath := flag.String("config", "config.yaml", "path to the YAML config file")
	flag.Parse()

	cfg, err := loadConfig(*configPath)
	if err != nil {
		log.Fatalf("failed to load config: %v", err)
	}

	if len(cfg.Queues) == 0 {
		log.Fatal("no queue mappings defined in config")
	}

	redisPassword := os.Getenv("REDIS_PASSWORD")

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	sigCh := make(chan os.Signal, 1)
	signal.Notify(sigCh, syscall.SIGINT, syscall.SIGTERM)
	go func() {
		sig := <-sigCh
		log.Printf("received signal %s, shutting down...", sig)
		cancel()
	}()

	var wg sync.WaitGroup
	for _, q := range cfg.Queues {
		wg.Add(1)
		go func(mapping QueueMapping) {
			defer wg.Done()
			if err := runWorker(ctx, cfg, redisPassword, mapping); err != nil {
				log.Printf("worker for list %q ended with error: %v", mapping.RedisList, err)
			}
		}(q)
	}

	wg.Wait()
	log.Println("all workers stopped, exiting")
}
