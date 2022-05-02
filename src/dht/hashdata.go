package dht

type HashKey struct {
	Hash int
	Key  string
}

type HashData struct {
	Data map[string]interface{}
}

func (hd *HashData) Put(key string, value interface{}) {
	if len(hd.Data) == 0 {
		hd.Data = make(map[string]interface{})
	}
	hd.Data[key] = value
}

func (hd *HashData) Get(key string) (interface{}, bool) {
	if len(hd.Data) == 0 {
		hd.Data = make(map[string]interface{})
	}
	val, ok := hd.Data[key]
	return val, ok
}

func (hd *HashData) AddRange(kvs map[string]interface{}) {
	if len(hd.Data) == 0 {
		hd.Data = make(map[string]interface{})
	}
	for k, v := range kvs {
		hd.Data[k] = v
	}
}
