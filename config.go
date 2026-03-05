package main

import (
	"fmt"
	"os"

	"gopkg.in/yaml.v3"
)

// Config holds all configuration for the service.
type Config struct {
	Redis struct {
		Host string `yaml:"host"`
		Port int    `yaml:"port"`
	} `yaml:"redis"`
	GCP struct {
		ProjectID string `yaml:"project_id"`
	} `yaml:"gcp"`
	Queues []QueueMapping `yaml:"queues"`
}

// QueueMapping maps a Redis list name to a GCP Pub/Sub topic name.
type QueueMapping struct {
	RedisList   string `yaml:"redis_list"`
	PubSubTopic string `yaml:"pubsub_topic"`
}

// loadConfig reads and parses the YAML config file at the given path.
func loadConfig(path string) (*Config, error) {
	data, err := os.ReadFile(path)
	if err != nil {
		return nil, fmt.Errorf("reading config file: %w", err)
	}
	var cfg Config
	if err := yaml.Unmarshal(data, &cfg); err != nil {
		return nil, fmt.Errorf("parsing config file: %w", err)
	}
	return &cfg, nil
}
