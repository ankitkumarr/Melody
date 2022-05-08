package dht

import (
	"errors"
)

type HashKey struct {
	Hash int
	Key  string
}

type HashData struct {
	Data     map[string]interface{}
	Versions map[string]int
}

//
// Only succeeds if version is -1 or matches what's stored
//
func (hd *HashData) Put(key string, value interface{}, version int) error {
	if len(hd.Data) == 0 {
		hd.Data = make(map[string]interface{})
		hd.Versions = make(map[string]int)
	}
	if version != -1 {
		if version != hd.Versions[key] {
			return errors.New("version did not match for put")
		}
	}
	hd.Data[key] = value
	hd.Versions[key] += 1
	return nil
}

//
// Does not care about the version, and overwrites the data.
//
func (hd *HashData) ForcePut(key string, value interface{}, version int) {
	hd.Put(key, value, version)
	if len(hd.Data) == 0 {
		hd.Data = make(map[string]interface{})
		hd.Versions = make(map[string]int)
	}
	hd.Data[key] = value
	hd.Versions[key] = version
}

func (hd *HashData) Get(key string) (interface{}, int, bool) {
	if len(hd.Data) == 0 {
		hd.Data = make(map[string]interface{})
		hd.Versions = make(map[string]int)
	}
	val, ok := hd.Data[key]
	version := hd.Versions[key]
	return val, version, ok
}

func (hd *HashData) AddRange(kvs map[string]interface{}, versions map[string]int) {
	if len(hd.Data) == 0 {
		hd.Data = make(map[string]interface{})
		hd.Versions = make(map[string]int)
	}
	for k, v := range kvs {
		hd.Data[k] = v
		hd.Versions[k] = versions[k]
	}
}

func (hd *HashData) RemoveRange(kvs map[string]interface{}, versions map[string]int) {
	if len(hd.Data) == 0 {
		return
	}
	for k := range kvs {
		delete(hd.Data, k)
		delete(hd.Versions, k)
	}
}
