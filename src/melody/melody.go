package melody

import (
	"fmt"
	"log"
	"strings"

	"../dht"
)

const KeywordPrefix = "__KEYWORD__"
const FilePrefix = "__FILE__"

type Melody struct {
	dht *dht.HashTableNode
}

type FileMetadata struct {
	Title string
	Id    string
}

func Make(dht *dht.HashTableNode) Melody {
	m := Melody{}
	m.dht = dht
	return m
}

func (m *Melody) AddFileToIndex(f FileMetadata) {
	keywords := strings.Fields(f.Title)

	for _, word := range keywords {
		key := fmt.Sprintf("%v%v", KeywordPrefix, word)

		// TODO: There's a race here. We may need to add a version number info in DHT, or
		// have the DHT itself supply and Append in place operation.
		// TODO: We need to make sure get and set operations are atomic.
		val := m.dht.Get(key)

		if val == nil {
			newval := make([]FileMetadata, 1)
			newval[0] = f
			m.dht.Put(key, newval)
		} else {
			if files, ok := val.([]FileMetadata); ok {
				files = append(files, f)
				m.dht.Put(key, files)
			} else {
				log.Fatalf("Invalid data in DHT. Expected File Metadata for key %v.", key)
			}
		}
	}
}

func (m *Melody) LookupFiles(query string) []string {
	keywords := strings.Fields(query)
	// TODO: We may want to add an upper bound to results.
	results := make([]string, 0)

	for _, word := range keywords {
		key := fmt.Sprintf("%v%v", KeywordPrefix, word)
		val := m.dht.Get(key)

		if val != nil {
			if files, ok := val.([]FileMetadata); ok {
				for _, file := range files {
					results = append(results, file.Title)
				}
			} else {
				log.Fatalf("Invalid data in DHT. Expected File Metadata for key %v.", key)
			}
		}
	}

	return results
}

func (m *Melody) AddPeerServingFile(peerAddress string, f FileMetadata) {
	key := fmt.Sprintf("%v%v", FilePrefix, f.Id)

	// TODO: There's a race here. We may need to add a version number info in DHT, or
	// have the DHT itself supply and Append in place operation.
	// TODO: We need to make sure get and set operations are atomic.
	val := m.dht.Get(key)

	if val == nil {
		newval := make([]string, 1)
		newval[0] = peerAddress
		m.dht.Put(key, newval)
	} else {
		if seeders, ok := val.([]string); ok {
			seeders = append(seeders, peerAddress)
			m.dht.Put(key, seeders)
		} else {
			log.Fatalf("Invalid data in DHT. Expected Seeders info for key %v.", key)
		}
	}
}

func (m *Melody) LocateSeeders(fileId string) []string {
	key := fmt.Sprintf("%v%v", FilePrefix, fileId)
	val := m.dht.Get(key)

	if val == nil {
		return nil
	}

	if seeders, ok := val.([]string); ok {
		return seeders
	} else {
		log.Fatalf("Invalid data in DHT. Expected Seeders info for key %v.", key)
		return nil
	}
}
