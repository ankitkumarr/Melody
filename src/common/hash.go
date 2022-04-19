package common

import (
	"bytes"
	"crypto/sha1"
	"encoding/binary"
	"math"
)

func KeyHash(key string) int {
	h := sha1.New()
	h.Write([]byte(key))

	bs := h.Sum(nil)
	buf := bytes.NewBuffer(bs[:])

	var sum int
	binary.Read(buf, binary.LittleEndian, &sum)

	return sum % int(math.Pow(2, 31))
}
