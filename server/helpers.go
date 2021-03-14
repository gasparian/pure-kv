package server

import (
	"hash/fnv"
)

func HashString(key string) uint64 {
	h := fnv.New64a()
	h.Write([]byte(key))
	return h.Sum64()
}

func GetKey(req core.Request) uint64 {
	key := req.IntKey
	if len(req.StringKey) > 0 {
		key = HashString(req.StringKey)
	}
	return key
}
