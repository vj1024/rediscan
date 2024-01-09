# rediscan
Scan key-value pairs in redis and use custom functions to process them.

## Getting started

example: [cmd/rediscan/main.go](cmd/rediscan/main.go)

```sh
git clone https://github.com/vj1024/rediscan.git
cd rediscan
go mod tidy && go build cmd/rediscan

# print all keys:
./rediscan

# print keys and values match some regexp:
./rediscan -key-regexp '^[0-9]+$' -value-regexp '^[a-zA-Z]+$'

# delete keys and values match some regexp:
./rediscan -key-regexp '^[0-9]+$' -value-regexp '^[a-zA-Z]+$' -cmd delete

# delete keys with ttl less than or equal to 60s:
./rediscan -cmd delete -ttl-lte 60s -ignore-value
```
