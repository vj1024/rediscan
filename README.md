# rediscan
Scan key-value pairs in redis and use custom functions to process them.

## Getting started

example: [cmd/rediscan.go](cmd/rediscan.go)

```sh
git clone https://github.com/vj1024/rediscan.git
cd rediscan
go mod tidy && go build cmd/*.go

# print all keys:
./rediscan

# print keys and values match some regexp:
./rediscan -key-regexp '^[0-9]+$' -value-regexp '^[a-zA-Z]+$'

# delete keys and values match some regexp:
./rediscan -key-regexp '^[0-9]+$' -value-regexp '^[a-zA-Z]+$' -cmd delete
```
