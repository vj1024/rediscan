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

	IgnoreValue bool
	ValueRegexp string
	valueRegexp *regexp.Regexp

	fromFlag bool
}

// ConfigFromFlags parse the config from command line.
// This function must be called before flag.Parse()
func ConfigFromFlags() *Config {
	conf := &Config{fromFlag: true}
	flag.StringVar(&conf.URL, "url", "redis://127.0.0.1:6379", "redis url, example: `redis://user:password@localhost:6789?dial_timeout=3s&db=0&read_timeout=6s&max_retries=2`")
	flag.Int64Var(&conf.Count, "count", 100, "limit count")
	flag.Uint64Var(&conf.Cursor, "cursor", 0, "start cursor")
	flag.StringVar(&conf.Match, "match", "", "match pattern, example: `*a*bc*`")
	flag.StringVar(&conf.KeyRegexp, "key-regexp", "", "match regexp pattern for key")
	flag.StringVar(&conf.ValueRegexp, "value-regexp", "", "match regexp pattern for value, not working when 'ignore-value' set to true")
	flag.DurationVar(&conf.Wait, "wait", 0, "time to wait between each scan, example: 1ms, 2s, 3m2s")
	flag.IntVar(&conf.Round, "round", 1, "Scan all keys up to N times and then exit")
	flag.BoolVar(&conf.IgnoreValue, "ignore-value", false, "do not get value for keys if set to true")

	return conf
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

	if conf.ValueRegexp != "" {
		conf.valueRegexp, err = regexp.Compile(conf.ValueRegexp)
		if err != nil {
			return fmt.Errorf("parse value-regexp err:%v", err)
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
	if count <= 0 {
		count = 100
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

			var val string
			var err error
			if !conf.IgnoreValue {
				val, err = db.Get(ctx, key).Result()
				if err != nil {
					logger("get key:%s, err:%v", key, err)
					continue
				}

				if conf.valueRegexp != nil && !conf.valueRegexp.MatchString(val) {
					continue
				}
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
