package main

import (
	"log"

	"github.com/go-redis/redis/v8"
	"github.com/vj1024/rediscan"
)

func main() {
	rediscan.Run(rediscan.ConfigFromFlags(), handler)
}

func handler(client *redis.Client, key, val string) {
	log.Printf("key:%q, val:%q", key, val)
}
