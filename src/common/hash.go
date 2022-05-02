package common

import (
	"crypto/sha1"
	"encoding/binary"
	"math"
)

func KeyHash(key string) int {
	// This isn't the best way to get a consistent int hash
	// But it will get the job done for our scope
	hashBytes := sha1.Sum([]byte(key))
	hashedInt64 := binary.BigEndian.Uint64(hashBytes[:])
	smallerInt64 := hashedInt64 % uint64(math.Pow(2, 31))
	return int(smallerInt64)
}
