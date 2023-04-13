package main

import (
	"context"
	"flag"
	"log"

	"github.com/go-redis/redis/v8"
	"github.com/vj1024/rediscan"
)

func main() {
	conf := rediscan.ConfigFromFlags()

	cmd := flag.String("cmd", "print", "print or delete")
	flag.Parse()

	var handler func(client *redis.Client, key, val string)

	switch *cmd {
	case "print":
		handler = printKV

	case "del", "delete":
		handler = deleteK

	default:
		log.Fatalf("invalid cmd: '%s'", *cmd)
	}

	err := rediscan.Run(conf, handler)
	if err != nil {
		log.Fatal(err)
	}
}

func printKV(_ *redis.Client, key, val string) {
	log.Printf("key:%q, val:%q", key, val)
}

func deleteK(c *redis.Client, key, _ string) {
	n, err := c.Del(context.Background(), key).Result()
	if err != nil {
		log.Printf("delete key:%s, error:%v", key, err)
	} else {
		log.Printf("delete key:%s, result:%d", key, n)
	}
}
