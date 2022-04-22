package melody

import (
	"crypto/rand"
	"encoding/gob"
	"encoding/json"
	"fmt"
	"io/ioutil"
	"log"
	"math/big"
	"net/http"
	"os"
	"regexp"
	"strings"

	"Melody/dht"
)

const KeywordPrefix = "__KEYWORD__"
const FilePrefix = "__FILE__"
const StoreDirectoryName = "MelodyFiles/"

type Melody struct {
	dht     *dht.HashTableNode
	address string
}

type FileMetadata struct {
	Title string
	Id    string
}

func Make(dht *dht.HashTableNode, myAdd string) Melody {
	m := Melody{}
	m.dht = dht
	m.address = myAdd
	gob.Register(&FileMetadata{})
	gob.Register([]FileMetadata{})
	m.setupHttpRoutes()
	return m
}

func getFile(w http.ResponseWriter, r *http.Request) {
	query := r.URL.Query()
	if len(query["fileid"]) == 0 {
		w.WriteHeader(http.StatusBadRequest)
		w.Write([]byte("Missing required query param 'fileid'"))
		return
	}

	fileid := query["fileid"][0]

	// This is important to also avoid attacks where people may pass filenames
	// such as "../../etc/passwd". Not today, hackers!
	is_alphanumeric := regexp.MustCompile(`^[a-zA-Z0-9]*$`).MatchString(fileid)
	if !is_alphanumeric {
		w.WriteHeader(http.StatusBadRequest)
		w.Write([]byte("'fileid' must only contain alpha-numeric characters"))
		return
	}

	if _, err := os.Stat(StoreDirectoryName + fileid); err == nil {
		// Ideally this would be streamed to the client.
		// However, loading in bytes in memory if ok for now for our prototype.
		fileBytes, err := ioutil.ReadFile(StoreDirectoryName + fileid)
		if err != nil {
			w.WriteHeader(http.StatusInternalServerError)
			w.Write([]byte("Failed to read file from disk"))
			return
		}
		w.Header().Set("Content-Type", "application/octet-stream")
		w.WriteHeader(http.StatusOK)
		w.Write(fileBytes)
	} else {
		w.WriteHeader(http.StatusNotFound)
		w.Write([]byte("Specified file was not found on this server"))
	}
}

func nrand() int64 {
	max := big.NewInt(int64(1) << 62)
	bigx, _ := rand.Int(rand.Reader, max)
	x := bigx.Int64()
	return x
}

func (m *Melody) addNewFile(w http.ResponseWriter, r *http.Request) {
	query := r.URL.Query()
	if len(query["filename"]) == 0 {
		w.WriteHeader(http.StatusBadRequest)
		w.Write([]byte("Missing required query param 'filename'"))
		return
	}

	filename := query["filename"][0]
	fileId := fmt.Sprintf("%v", nrand())

	// Ideally this would be streamed to the client.
	// However, loading in bytes in memory if ok for now for our prototype.
	filedata, err := ioutil.ReadAll(r.Body)
	if err != nil {
		w.WriteHeader(http.StatusBadRequest)
		w.Write([]byte("Could not read the file sent in the request"))
		return
	}

	os.MkdirAll(StoreDirectoryName, os.ModePerm)
	f, err := os.Create(StoreDirectoryName + fileId)
	if err != nil {
		w.WriteHeader(http.StatusInternalServerError)
		w.Write([]byte("Error writing the file to storage"))
		return
	}
	defer f.Close()
	f.Write(filedata)
	w.WriteHeader(http.StatusOK)
	w.Write([]byte(fileId))

	newfile := FileMetadata{Id: fileId, Title: filename}
	m.AddFileToIndex(newfile)
	m.AddPeerServingFile(m.address, newfile)
}

func (m *Melody) queryFiles(w http.ResponseWriter, r *http.Request) {
	query := r.URL.Query()
	if len(query["query"]) == 0 {
		w.WriteHeader(http.StatusBadRequest)
		w.Write([]byte("Missing required query param 'query'"))
		return
	}

	searchQuery := query["query"][0]
	files := m.LookupFiles(searchQuery)
	filesjson, err := json.Marshal(files)
	if err != nil {
		w.WriteHeader(http.StatusInternalServerError)
		w.Write([]byte("Error encoding file Ids to json"))
		return
	}

	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(http.StatusOK)
	w.Write([]byte(filesjson))
}

func (m *Melody) findFile(w http.ResponseWriter, r *http.Request) {
	query := r.URL.Query()
	if len(query["fileId"]) == 0 {
		w.WriteHeader(http.StatusBadRequest)
		w.Write([]byte("Missing required query param 'fileId'"))
		return
	}

	fileId := query["fileId"][0]

	// This is important to also avoid attacks where people may pass fileIds
	// such as "../../etc/passwd". Not today, hackers!
	is_alphanumeric := regexp.MustCompile(`^[a-zA-Z0-9]*$`).MatchString(fileId)
	if !is_alphanumeric {
		w.WriteHeader(http.StatusBadRequest)
		w.Write([]byte("'fileId' must only contain alpha-numeric characters"))
		return
	}

	seeders := m.LocateSeeders(fileId)
	seedersJson, err := json.Marshal(seeders)
	if err != nil {
		w.WriteHeader(http.StatusInternalServerError)
		w.Write([]byte("Error encoding seeders to json"))
		return
	}

	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(http.StatusOK)
	w.Write([]byte(seedersJson))
}

func (m *Melody) setupHttpRoutes() {
	http.HandleFunc("/queryfiles", m.queryFiles)
	http.HandleFunc("/findfilelocation", m.findFile)
	http.HandleFunc("/addnewfile", m.addNewFile)
	http.HandleFunc("/getfile", getFile)
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
				log.Fatalf("Invalid data in DHT. Expected File Metadata for key %v. Found %v", key, val)
			}
		}
	}
}

func (m *Melody) LookupFiles(query string) []FileMetadata {
	keywords := strings.Fields(query)
	// TODO: We may want to add an upper bound to results.
	results := make([]FileMetadata, 0)

	for _, word := range keywords {
		key := fmt.Sprintf("%v%v", KeywordPrefix, word)
		val := m.dht.Get(key)

		if val != nil {
			if files, ok := val.([]FileMetadata); ok {
				results = append(results, files...)
			} else {
				log.Fatalf("Invalid data in DHT. Expected File Metadata for key %v. Found %v", key, val)
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
