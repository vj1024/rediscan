package rediscan

import (
	"context"
	"errors"
	"flag"
	"fmt"
	"log"
	"os"
	"regexp"
	"time"

	"github.com/go-redis/redis/v8"
)

// Logger to print logs.
var logger = log.Printf

// Config is the redis scan config.
type Config struct {
	URL    string
	Count  int64
	Cursor uint64
	Match  string
	Wait   time.Duration
	Round  int

	KeyRegexp string
	keyRegexp *regexp.Regexp

	fromFlag bool
}

// ConfigFromFlags parse the config from command line.
// This function must be called before flag.Parse()
func ConfigFromFlags() *Config {
	flags := &Config{fromFlag: true}
	flag.StringVar(&flags.URL, "url", "redis://127.0.0.1:6379", "redis url, example: `redis://user:password@localhost:6789/3?dial_timeout=3&db=1&read_timeout=6s&max_retries=2`")
	flag.Int64Var(&flags.Count, "count", 100, "limit count")
	flag.Uint64Var(&flags.Cursor, "cursor", 0, "start cursor")
	flag.StringVar(&flags.Match, "match", "", "match pattern, example: `*a*bc*`")
	flag.StringVar(&flags.KeyRegexp, "key-regexp", "", "match regexp pattern for key")
	flag.DurationVar(&flags.Wait, "wait", 0, "time to wait between each scan, example: 1ms, 2s, 3m2s")
	flag.IntVar(&flags.Round, "round", 1, "Scan all keys up to N times and then exit")

	return flags
}

// Run the redis scan.
func Run(conf *Config, handler func(client *redis.Client, key, val string)) error {
	if conf == nil {
		return errors.New("config is nil")
	}
	if conf.fromFlag && !flag.Parsed() {
		flag.Parse()
	}

	var err error
	if conf.KeyRegexp != "" {
		conf.keyRegexp, err = regexp.Compile(conf.KeyRegexp)
		if err != nil {
			return fmt.Errorf("parse key-regexp err:%v", err)
		}
	}

	o, err := redis.ParseURL(conf.URL)
	if err != nil {
		return fmt.Errorf("parse url err:%v", err)
	}
	db := redis.NewClient(o)
	if err := db.Ping(context.Background()).Err(); err != nil {
		return fmt.Errorf("ping err:%v", err)
	}

	scanKeys(db, conf, handler)
	return nil
}

// Logger set the logger to print logs.
func Logger(l func(format string, args ...interface{})) {
	if l != nil {
		logger = l
	}
}

func scanKeys(db *redis.Client, conf *Config, handler func(client *redis.Client, key, val string)) {
	round := 0
	errCount := 0
	keyCount := 0
	ctx := context.Background()

	cursor := conf.Cursor
	count := conf.Count
	if count < 1 {
		count = 1
	}

	for {
		keys, next, err := db.Scan(ctx, cursor, conf.Match, count).Result()
		if err != nil {
			errCount++
			logger("scan cursor:%d, err:%v", cursor, err)
			if errCount >= 5 {
				os.Exit(1)
			}
			time.Sleep(time.Second)
			continue
		}

		errCount = 0
		keyCount += len(keys)
		logger("round:%d, scan cursor:%d, next:%d, keys:%d, total keys:%d", round+1, cursor, next, len(keys), keyCount)
		cursor = next

		for _, key := range keys {
			if conf.keyRegexp != nil && !conf.keyRegexp.MatchString(key) {
				continue
			}

			val, err := db.Get(ctx, key).Result()
			if err != nil {
				logger("get key:%s, err:%v", key, err)
				continue
			}
			handler(db, key, val)
		}

		if next == 0 {
			round++
			if round >= conf.Round && conf.Round > 0 {
				return
			}

			keyCount = 0
			continue
		}

		if conf.Wait > 0 {
			time.Sleep(conf.Wait)
		}
	}
}
