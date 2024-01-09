package main

import (
	"context"
	"flag"
	"fmt"
	"log/slog"
	"os"
	"time"

	"github.com/go-redis/redis/v8"
	"github.com/vj1024/rediscan"
)

func main() {
	conf := rediscan.ConfigFromFlags()

	cmd := flag.String("cmd", "print", "print or delete")
	logLevel := flag.String("log", "INFO", "set log level to one of DEBUG/INFO/WARN/ERROR")
	flag.Parse()

	l := slog.LevelInfo
	if err := l.UnmarshalText([]byte(*logLevel)); err != nil {
		fmt.Fprintf(os.Stderr, "invalid log level: '%s'\n", *logLevel)
		return
	}
	slog.SetDefault(slog.New(slog.NewJSONHandler(os.Stdout, &slog.HandlerOptions{Level: l})))

	var handler rediscan.Handler

	switch *cmd {
	case "print":
		handler = printKV

	case "del", "delete":
		handler = deleteK

	default:
		fmt.Fprintf(os.Stderr, "invalid cmd: '%s'\n", *cmd)
		return
	}

	err := rediscan.Run(conf, handler)
	if err != nil {
		fmt.Fprintln(os.Stderr, "run error:", err)
		return
	}
}

func ttlToSeconds(ttl *time.Duration) *int {
	if ttl == nil {
		return nil
	}
	if *ttl > 0 {
		n := int(*ttl / time.Second)
		return &n
	}
	n := int(*ttl)
	return &n
}

func printKV(_ *redis.Client, key string, val *string, ttl *time.Duration) {
	slog.With("key", key, "value", val, "ttl", ttlToSeconds(ttl)).Info("print")
	//slog.Info("print", slog.String("key", key), slog.String("value", val))
}

func deleteK(c *redis.Client, key string, val *string, ttl *time.Duration) {
	log := slog.With("key", key, "value", val, "ttl", ttlToSeconds(ttl))
	n, err := c.Del(context.Background(), key).Result()
	if err != nil {
		log.With("error", err).Error("delete error")
	} else {
		log.With("result", n).Info("delete ok")
	}
}
