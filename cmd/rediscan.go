package main

import (
	"log"

	"github.com/go-redis/redis/v8"
	"github.com/vj1024/rediscan"
)

func main() {
	err := rediscan.Run(rediscan.ConfigFromFlags(), handler)
	if err != nil {
		log.Fatal(err)
	}
}

func handler(_ *redis.Client, key, val string) {
	log.Printf("key:%q, val:%q", key, val)
}
