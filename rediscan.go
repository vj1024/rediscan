package rediscan

import (
	"context"
	"errors"
	"flag"
	"fmt"
	"log/slog"
	"os"
	"regexp"
	"time"

	"github.com/go-redis/redis/v8"
)

// Version is the program version set on build.
var Version = "unknown"

// Config is the redis scan config.
type Config struct {
	URL    string
	Count  int64
	Cursor uint64
	Match  string
	Wait   time.Duration
	Round  int
	Limit  int64

	KeyRegexp string
	keyRegexp *regexp.Regexp

	IgnoreValue bool
	ValueRegexp string
	valueRegexp *regexp.Regexp

	TtlGte    string
	TtlLte    string
	ttlFilter func(d time.Duration) bool

	PrintVersion bool

	fromFlag bool
}

// ConfigFromFlags parse the config from command line.
// This function must be called before flag.Parse()
func ConfigFromFlags() *Config {
	conf := &Config{fromFlag: true}
	flag.StringVar(&conf.URL, "url", "redis://127.0.0.1:6379", "redis url, example: 'redis://user:password@localhost:6789?dial_timeout=3s&db=0&read_timeout=6s&max_retries=2'")
	flag.Int64Var(&conf.Count, "count", 100, "limit count")
	flag.Uint64Var(&conf.Cursor, "cursor", 0, "start cursor")
	flag.StringVar(&conf.Match, "match", "", "match pattern, example: '*a*bc*'")
	flag.StringVar(&conf.KeyRegexp, "key-regexp", "", "match regexp pattern for key")
	flag.StringVar(&conf.ValueRegexp, "value-regexp", "", "match regexp pattern for value, not working when 'ignore-value' set to true")
	flag.DurationVar(&conf.Wait, "wait", 0, "time to wait between each scan, example: 1ms, 2s, 3m2s")
	flag.IntVar(&conf.Round, "round", 1, "scan all keys up to N times and then exit")
	flag.Int64Var(&conf.Limit, "limit", 0, "if set to N and N > 0, exit process after handled N keys")
	flag.BoolVar(&conf.IgnoreValue, "ignore-value", false, "do not get value for keys if set to true")
	flag.StringVar(&conf.TtlGte, "ttl-gte", "", "if set, filter keys with ttl greater than or equal to the duration, duration example: 10s")
	flag.StringVar(&conf.TtlLte, "ttl-lte", "", "if set, filter keys with ttl less than or equal to the duration, duration example: 10m")
	flag.BoolVar(&conf.PrintVersion, "v", false, "print version and exit")

	return conf
}

// Handler to handle keys and values.
//
// `val` is nil when flag `ignore-value` is set.
// `ttl` is nil when flags `ttl-gte` and `ttl-lte` are both not set.
type Handler func(client *redis.Client, key string, val *string, ttl *time.Duration)

// Run the redis scan.
func Run(conf *Config, handler Handler) error {
	if conf == nil {
		return errors.New("config is nil")
	}
	if conf.fromFlag && !flag.Parsed() {
		flag.Parse()
	}

	if conf.PrintVersion {
		fmt.Println(Version)
		return nil
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

	if conf.TtlGte != "" || conf.TtlLte != "" {
		var gte, lte *time.Duration
		if conf.TtlGte != "" {
			d, err := time.ParseDuration(conf.TtlGte)
			if err != nil {
				return fmt.Errorf("parse ttl-gte to duration err: %v", err)
			}
			gte = &d
		}
		if conf.TtlLte != "" {
			d, err := time.ParseDuration(conf.TtlLte)
			if err != nil {
				return fmt.Errorf("parse ttl-lte to duration err: %v", err)
			}
			lte = &d
		}
		conf.ttlFilter = func(d time.Duration) bool {
			if gte != nil && d < *gte {
				return false
			}
			if lte != nil && d > *lte {
				return false
			}
			return true
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

func scanKeys(db *redis.Client, conf *Config, handler Handler) {
	round := 0
	errCount := 0
	totalKeys := 0
	ctx := context.Background()

	var handledCount int64

	cursor := conf.Cursor
	count := conf.Count
	if count <= 0 {
		count = 100
	}

	for {
		keys, next, err := db.Scan(ctx, cursor, conf.Match, count).Result()
		if err != nil {
			errCount++
			slog.With("cursor", cursor, "error", err).Error("scan error")
			if errCount >= 5 {
				os.Exit(1)
			}
			time.Sleep(time.Second)
			continue
		}

		errCount = 0
		totalKeys += len(keys)
		slog.With("round", round+1, "cursor", cursor, "next", next, "keys", len(keys), "total_keys", totalKeys).Debug("")
		cursor = next

		for _, key := range keys {
			if conf.keyRegexp != nil && !conf.keyRegexp.MatchString(key) {
				continue
			}

			var value *string
			if !conf.IgnoreValue {
				v, err := db.Get(ctx, key).Result()
				if err != nil {
					slog.With("error", err, "key", key).Error("get key error")
					continue
				}
				value = &v

				if conf.valueRegexp != nil && !conf.valueRegexp.MatchString(v) {
					continue
				}
			}

			var ttl *time.Duration
			if conf.ttlFilter != nil {
				v, err := db.TTL(ctx, key).Result()
				if err != nil {
					slog.With("key", key, "error", err).Error("get ttl error")
					continue
				}
				ttl = &v

				if !conf.ttlFilter(v) {
					continue
				}
			}

			handler(db, key, value, ttl)
			handledCount++
			if conf.Limit > 0 && handledCount >= conf.Limit {
				return
			}
		}

		if next == 0 {
			round++
			if round >= conf.Round && conf.Round > 0 {
				return
			}

			totalKeys = 0
			continue
		}

		if conf.Wait > 0 {
			time.Sleep(conf.Wait)
		}
	}
}
