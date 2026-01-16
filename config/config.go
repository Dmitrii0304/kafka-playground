package config

import "os"

type Config struct {
	Broker string
	Topic  string
	Group  string
}

func Load() Config {
	return Config{
		Broker: env("KAFKA_BROKER", "localhost:9092"),
		Topic:  env("KAFKA_TOPIC", "demo-topic"),
		Group:  env("KAFKA_GROUP", "demo-group"),
	}
}

func env(key, def string) string {
	if v := os.Getenv(key); v != "" {
		return v
	}
	return def
}
