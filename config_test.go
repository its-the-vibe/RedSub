package main

import (
	"os"
	"path/filepath"
	"testing"
)

func TestLoadConfig_Valid(t *testing.T) {
	content := `
redis:
  host: localhost
  port: 6379
gcp:
  project_id: my-project
queues:
  - redis_list: my-list
    pubsub_topic: my-topic
`
	path := filepath.Join(t.TempDir(), "config.yaml")
	if err := os.WriteFile(path, []byte(content), 0600); err != nil {
		t.Fatalf("writing temp config: %v", err)
	}

	cfg, err := loadConfig(path)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if cfg.Redis.Host != "localhost" {
		t.Errorf("expected redis host %q, got %q", "localhost", cfg.Redis.Host)
	}
	if cfg.Redis.Port != 6379 {
		t.Errorf("expected redis port %d, got %d", 6379, cfg.Redis.Port)
	}
	if cfg.GCP.ProjectID != "my-project" {
		t.Errorf("expected project_id %q, got %q", "my-project", cfg.GCP.ProjectID)
	}
	if len(cfg.Queues) != 1 {
		t.Fatalf("expected 1 queue mapping, got %d", len(cfg.Queues))
	}
	if cfg.Queues[0].RedisList != "my-list" {
		t.Errorf("expected redis_list %q, got %q", "my-list", cfg.Queues[0].RedisList)
	}
	if cfg.Queues[0].PubSubTopic != "my-topic" {
		t.Errorf("expected pubsub_topic %q, got %q", "my-topic", cfg.Queues[0].PubSubTopic)
	}
}

func TestLoadConfig_FileNotFound(t *testing.T) {
	_, err := loadConfig("/nonexistent/path/config.yaml")
	if err == nil {
		t.Fatal("expected error for missing file, got nil")
	}
}

func TestLoadConfig_InvalidYAML(t *testing.T) {
	path := filepath.Join(t.TempDir(), "config.yaml")
	if err := os.WriteFile(path, []byte(":\tinvalid: yaml: [\n"), 0600); err != nil {
		t.Fatalf("writing temp config: %v", err)
	}
	_, err := loadConfig(path)
	if err == nil {
		t.Fatal("expected error for invalid YAML, got nil")
	}
}

func TestLoadConfig_MultipleQueues(t *testing.T) {
	content := `
redis:
  host: redis-host
  port: 6380
gcp:
  project_id: proj
queues:
  - redis_list: list-a
    pubsub_topic: topic-a
  - redis_list: list-b
    pubsub_topic: topic-b
`
	path := filepath.Join(t.TempDir(), "config.yaml")
	if err := os.WriteFile(path, []byte(content), 0600); err != nil {
		t.Fatalf("writing temp config: %v", err)
	}

	cfg, err := loadConfig(path)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if len(cfg.Queues) != 2 {
		t.Fatalf("expected 2 queue mappings, got %d", len(cfg.Queues))
	}
}
