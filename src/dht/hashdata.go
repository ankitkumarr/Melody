package dht

type HashKey struct {
	Hash int
	Key  string
}

type HashData struct {
	// HashToKey []HashKey
	Data map[string]interface{}
}

// func binarySearch(sorted []HashKey, el HashKey) int {
// 	idx := -1
// 	start := 0
// 	end := len(sorted) - 1

// 	for start <= end {
// 		mid := start + ((end - start) / 2)
// 		if sorted[mid].Hash == el.Hash {
// 			idx = mid
// 			break
// 		} else if sorted[mid].Hash < el.Hash {
// 			start = mid + 1
// 		} else if sorted[mid].Hash > el.Hash {
// 			end = mid - 1
// 		}
// 	}

// 	return idx
// }

// TODO: Don't need sorting
//
// // This can probably be optimized further by using a binary search tree
// // However, go doesn't have a standard implementation, and we don't
// // need it at this stage.
// // The function does use binary search to at least improve some perf.
// //
// func sortedInsert(sorted []HashKey, el HashKey) []HashKey {
// 	i := binarySearch(sorted, el)
// 	sorted = append(sorted, HashKey{})
// 	copy(sorted[i+1:], sorted[i:])
// 	sorted[i] = el
// 	return sorted
// }

func (hd *HashData) Put(key string, value interface{}) {
	if len(hd.Data) == 0 {
		hd.Data = make(map[string]interface{})
	}
	hd.Data[key] = value
	// newKey := HashKey{Key: key, Hash: common.KeyHash(key)}
	// hd.HashToKey = sortedInsert(hd.HashToKey, newKey)
}

func (hd *HashData) Get(key string) (interface{}, bool) {
	if len(hd.Data) == 0 {
		hd.Data = make(map[string]interface{})
	}
	val, ok := hd.Data[key]
	return val, ok
}

// func (hd *HashData) GetKeysInRange(keyStart int, keyEnd int) map[string]interface{} {
// 	data := make(map[string]interface{})
// 	for _, k := range hd.HashToKey {
// 		if k.Hash >= keyStart && k.Hash <= keyEnd {
// 			data[k.Key] = hd.Data[k.Key]
// 		}
// 	}
// 	return data
// }

// func (hd *HashData) RemoveKeysInRange(keyStart int, keyEnd int) {
// 	data := make(map[string]interface{})
// 	hashKeyData := make([]HashKey, 0)
// 	for _, k := range hd.HashToKey {
// 		if k.Hash >= keyStart && k.Hash <= keyEnd {
// 			continue
// 		}
// 		data[k.Key] = hd.Data[k.Key]
// 		hashKeyData = append(hashKeyData, k)
// 	}

// 	hd.Data = data
// 	hd.HashToKey = hashKeyData
// }

func (hd *HashData) AddRange(kvs map[string]interface{}) {
	if len(hd.Data) == 0 {
		hd.Data = make(map[string]interface{})
	}
	for k, v := range kvs {
		hd.Data[k] = v
		// hd.HashToKey = sortedInsert(hd.HashToKey, HashKey{Hash: common.KeyHash(k), Key: k})
	}
}
