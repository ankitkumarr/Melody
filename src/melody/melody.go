package melody

import (
	"crypto/rand"
	"crypto/sha256"
	"encoding/gob"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"io"
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
const StoreDirectoryBase = "MelodyFiles"
const DhtRetries = 5

type Melody struct {
	dht     *dht.HashTableNode
	id      int
	address string
}

type FileMetadata struct {
	Title string
	Id    string
	Hash  string
}

type FileSeederInfo struct {
	Metadata FileMetadata
	Seeders  []string
}

const infolog = true

func InfoLog(format string, a ...interface{}) {
	if infolog {
		log.Printf(format, a...)
	}
}

func Make(dht *dht.HashTableNode, hashedId int, myAdd string) Melody {
	m := Melody{}
	m.dht = dht
	m.id = hashedId
	m.address = myAdd
	gob.Register(&FileMetadata{})
	gob.Register([]FileMetadata{})
	gob.Register(FileSeederInfo{})
	m.setupHttpRoutes()
	return m
}

func (m *Melody) getFile(w http.ResponseWriter, r *http.Request) {
	query := r.URL.Query()
	w.Header().Set("Access-Control-Allow-Origin", "*")
	if len(query["fileid"]) == 0 {
		w.WriteHeader(http.StatusBadRequest)
		w.Write([]byte("Missing required query param 'fileid'"))
		return
	}

	fileid := query["fileid"][0]

	InfoLog("MELODY revceived request to get file with id %v.", fileid)

	// This is important to also avoid attacks where people may pass filenames
	// such as "../../etc/passwd". Not today, hackers!
	is_alphanumeric := regexp.MustCompile(`^[a-zA-Z0-9]*$`).MatchString(fileid)
	if !is_alphanumeric {
		w.WriteHeader(http.StatusBadRequest)
		w.Write([]byte("'fileid' must only contain alpha-numeric characters"))
		return
	}

	if _, err := os.Stat(m.getStoreDirectoryName() + fileid); err == nil {
		// Ideally this would be streamed to the client.
		// However, loading in bytes in memory if ok for now for our prototype.
		fileBytes, err := ioutil.ReadFile(m.getStoreDirectoryName() + fileid)
		if err != nil {
			w.WriteHeader(http.StatusInternalServerError)
			w.Write([]byte("Failed to read file from disk"))
			return
		}

		filedata := m.LocateSeeders(fileid)

		if filedata.Metadata.Id != fileid {
			w.WriteHeader(http.StatusBadRequest)
			w.Write([]byte("This file (although stored) is not listed in the index."))
			return
		}

		w.Header().Set("Content-Type", "application/octet-stream")
		w.Header().Set("Content-Disposition", "attachment; filename="+filedata.Metadata.Title)
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

func (m *Melody) addNewFileRaw(w http.ResponseWriter, r *http.Request) {
	query := r.URL.Query()
	w.Header().Set("Access-Control-Allow-Origin", "*")
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

	os.MkdirAll(m.getStoreDirectoryName(), os.ModePerm)
	f, err := os.Create(m.getStoreDirectoryName() + fileId)
	if err != nil {
		w.WriteHeader(http.StatusInternalServerError)
		w.Write([]byte("Error writing the file to storage"))
		return
	}
	defer f.Close()
	f.Write(filedata)
	w.WriteHeader(http.StatusOK)
	w.Write([]byte(fileId))

	hash := sha256.Sum256(filedata)

	newfile := FileMetadata{Id: fileId, Title: filename, Hash: hex.EncodeToString(hash[:])}
	m.AddFileToIndex(newfile)
	m.AddPeerServingFile(m.address, newfile)
}

func (m *Melody) addNewFileForm(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Access-Control-Allow-Origin", "*")

	err := r.ParseMultipartForm(32 << 20)
	if err != nil {
		w.WriteHeader(http.StatusBadRequest)
		w.Write([]byte("Could not parse form data"))
		return
	}

	filename := r.Form.Get("name")
	fileId := fmt.Sprintf("%v", nrand())

	if filename == "" {
		w.WriteHeader(http.StatusBadRequest)
		w.Write([]byte("Could read the file name from file"))
		return
	}

	InfoLog("MELODY Received request to add new file with title %v", filename)

	receivedFile, _, err := r.FormFile("file")

	if err != nil {
		w.WriteHeader(http.StatusBadRequest)
		w.Write([]byte("Could not read file from the form data"))
		return
	}

	os.MkdirAll(m.getStoreDirectoryName(), os.ModePerm)
	f, err := os.Create(m.getStoreDirectoryName() + fileId)
	if err != nil {
		w.WriteHeader(http.StatusInternalServerError)
		w.Write([]byte("Error writing the file to storage"))
		return
	}
	defer f.Close()
	re := io.TeeReader(receivedFile, f)

	hash := sha256.New()
	if _, err := io.Copy(hash, re); err != nil {
		w.WriteHeader(http.StatusInternalServerError)
		w.Write([]byte("Could not compute the hash of the uploaded file"))
		return
	}

	newfile := FileMetadata{Id: fileId, Title: filename, Hash: hex.EncodeToString(hash.Sum(nil))}
	m.AddFileToIndex(newfile)
	m.AddPeerServingFile(m.address, newfile)

	w.WriteHeader(http.StatusOK)
	w.Write([]byte(fileId))
}

func (m *Melody) submitFileForSeedingForm(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Access-Control-Allow-Origin", "*")

	err := r.ParseMultipartForm(32 << 20)
	if err != nil {
		w.WriteHeader(http.StatusBadRequest)
		w.Write([]byte("Could not parse form data"))
		return
	}

	fileId := r.Form.Get("fileid")

	if fileId == "" {
		w.WriteHeader(http.StatusBadRequest)
		w.Write([]byte("Could not read file name the form data"))
		return
	}

	InfoLog("MELODY received request for submitting file for seeding with ID %v", fileId)

	receivedFile, _, err := r.FormFile("file")

	if err != nil {
		w.WriteHeader(http.StatusBadRequest)
		w.Write([]byte("Could not read file from the form data"))
		return
	}

	fsi := m.LocateSeeders(fileId)
	if fsi.Metadata.Id == "" || fsi.Metadata.Id != fileId {
		w.WriteHeader(http.StatusBadRequest)
		w.Write([]byte("Could not find the fileId in the network"))
		return
	}
	os.MkdirAll(m.getStoreDirectoryName(), os.ModePerm)

	files, err := ioutil.ReadDir(m.getStoreDirectoryName())
	if err != nil {
		w.WriteHeader(http.StatusInternalServerError)
		w.Write([]byte("Could not find the files being seeded"))
		return
	}

	for _, f := range files {
		if f.Name() == fileId {
			w.WriteHeader(http.StatusBadRequest)
			w.Write([]byte("Already seeding file with the given ID"))
			return
		}
	}

	f, err := os.Create(m.getStoreDirectoryName() + fileId)
	if err != nil {
		w.WriteHeader(http.StatusInternalServerError)
		w.Write([]byte("Error writing the file to storage"))
		return
	}
	nr := io.TeeReader(receivedFile, f)

	hash := sha256.New()
	if _, err := io.Copy(hash, nr); err != nil {
		w.WriteHeader(http.StatusInternalServerError)
		w.Write([]byte("Could not compute the hash of the uploaded file"))
		f.Close()
		return
	}

	hexhash := hex.EncodeToString(hash.Sum(nil))
	if hexhash != fsi.Metadata.Hash {
		w.WriteHeader(http.StatusBadRequest)
		w.Write([]byte("The hash of the seeded file did not match the hash stored."))
		f.Close()
		os.Remove(m.getStoreDirectoryName() + fileId)
		return
	}

	defer f.Close()
	w.WriteHeader(http.StatusOK)
	w.Write([]byte(fileId))
	m.AddPeerServingFile(m.address, fsi.Metadata)
}

//
// This method is for adding the file as a binary data in the request.
//
// func (m *Melody) submitFileForSeeding(w http.ResponseWriter, r *http.Request) {
// 	query := r.URL.Query()
// 	w.Header().Set("Access-Control-Allow-Origin", "*")
// 	if len(query["fileid"]) == 0 {
// 		w.WriteHeader(http.StatusBadRequest)
// 		w.Write([]byte("Missing required query param 'fileid'"))
// 		return
// 	}

// 	fileId := query["fileid"][0]

// 	fsi := m.LocateSeeders(fileId)
// 	if fsi.Metadata.Id == "" || fsi.Metadata.Id != fileId {
// 		w.WriteHeader(http.StatusBadRequest)
// 		w.Write([]byte("Could not find the fileId in the network"))
// 		return
// 	}

// 	// Ideally this would be streamed to the client.
// 	// However, loading in bytes in memory if ok for now for our prototype.
// 	filedata, err := ioutil.ReadAll(r.Body)
// 	if err != nil {
// 		w.WriteHeader(http.StatusBadRequest)
// 		w.Write([]byte("Could not read the file sent in the request"))
// 		return
// 	}

// 	os.MkdirAll(m.getStoreDirectoryName(), os.ModePerm)

// 	files, err := ioutil.ReadDir(m.getStoreDirectoryName())
// 	if err != nil {
// 		w.WriteHeader(http.StatusInternalServerError)
// 		w.Write([]byte("Could not find the files being seeded"))
// 		return
// 	}

// 	for _, f := range files {
// 		if f.Name() == fileId {
// 			w.WriteHeader(http.StatusBadRequest)
// 			w.Write([]byte("Already seeding file with the given ID"))
// 			return
// 		}
// 	}

// 	f, err := os.Create(m.getStoreDirectoryName() + fileId)
// 	if err != nil {
// 		w.WriteHeader(http.StatusInternalServerError)
// 		w.Write([]byte("Error writing the file to storage"))
// 		return
// 	}
// 	defer f.Close()
// 	f.Write(filedata)

// 	w.WriteHeader(http.StatusOK)
// 	w.Write([]byte(fileId))
// 	m.AddPeerServingFile(m.address, fsi.Metadata)
// }

func (m *Melody) getFilesSeeding(w http.ResponseWriter, r *http.Request) {
	os.MkdirAll(m.getStoreDirectoryName(), os.ModePerm)
	files, err := ioutil.ReadDir(m.getStoreDirectoryName())
	w.Header().Set("Access-Control-Allow-Origin", "*")

	if err != nil {
		w.WriteHeader(http.StatusInternalServerError)
		w.Write([]byte("Could not find the files being seeded"))
		return
	}

	result := make([]string, len(files))
	for i, f := range files {
		result[i] = f.Name()
	}

	resultJson, err := json.Marshal(result)
	if err != nil {
		w.WriteHeader(http.StatusInternalServerError)
		w.Write([]byte("Error encoding seeding files to json"))
		return
	}

	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(http.StatusOK)
	w.Write([]byte(resultJson))
}

func (m *Melody) getNextNodes(w http.ResponseWriter, r *http.Request) {
	nextNodes := dht.GetSuccessors(m.dht)
	nextNodesJson, err := json.Marshal(nextNodes)
	w.Header().Set("Access-Control-Allow-Origin", "*")
	if err != nil {
		w.WriteHeader(http.StatusInternalServerError)
		w.Write([]byte("Error encoding next nodes to json"))
		return
	}

	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(http.StatusOK)
	w.Write([]byte(nextNodesJson))
}

func (m *Melody) getAllLocalKeywords(w http.ResponseWriter, r *http.Request) {
	localdata := dht.GetData(m.dht)
	result := make(map[string][]FileMetadata)
	w.Header().Set("Access-Control-Allow-Origin", "*")

	for k, v := range localdata {
		if strings.HasPrefix(k, KeywordPrefix) {
			val, ok := v.([]FileMetadata)
			if !ok {
				w.WriteHeader(http.StatusInternalServerError)
				w.Write([]byte("Unexpected data when reading local data."))
				return
			}
			result[k] = val
		}
	}

	resultJson, err := json.Marshal(result)
	if err != nil {
		w.WriteHeader(http.StatusInternalServerError)
		w.Write([]byte("Error encoding result to json"))
		return
	}

	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(http.StatusOK)
	w.Write([]byte(resultJson))
}

func (m *Melody) queryFiles(w http.ResponseWriter, r *http.Request) {
	query := r.URL.Query()
	w.Header().Set("Access-Control-Allow-Origin", "*")
	if len(query["query"]) == 0 {
		w.WriteHeader(http.StatusBadRequest)
		w.Write([]byte("Missing required query param 'query'"))
		return
	}

	searchQuery := query["query"][0]

	InfoLog("MELODY revceived request to search files with query %v.", searchQuery)

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
	w.Header().Set("Access-Control-Allow-Origin", "*")
	if len(query["fileId"]) == 0 {
		w.WriteHeader(http.StatusBadRequest)
		w.Write([]byte("Missing required query param 'fileId'"))
		return
	}

	fileId := query["fileId"][0]

	InfoLog("MELODY revceived request to lookup file with id %v.", fileId)

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
	// APIs for client
	http.HandleFunc("/queryfiles", m.queryFiles)
	http.HandleFunc("/findfile", m.findFile)
	http.HandleFunc("/addnewfileform", m.addNewFileForm)
	http.HandleFunc("/getfile", m.getFile)
	http.HandleFunc("/submitfileforseedingform", m.submitFileForSeedingForm)

	// Melody client
	http.Handle("/", http.FileServer(http.Dir("../client")))

	// raw requests (easy curl) support
	http.HandleFunc("/addnewfileraw", m.addNewFileRaw)
	// http.HandleFunc("/submitfileforseeding", m.submitFileForSeeding)

	// Support for authority mostly
	http.HandleFunc("/getfilesseeding", m.getFilesSeeding)
	http.HandleFunc("/getnextnodes", m.getNextNodes)
	http.HandleFunc("/getlocalkeywords", m.getAllLocalKeywords)
}

func (m *Melody) getStoreDirectoryName() string {
	return StoreDirectoryBase + "_" + fmt.Sprint(m.id) + "/"
}

func (m *Melody) AddFileToIndex(f FileMetadata) {
	keywords := strings.Fields(f.Title)

	for _, word := range keywords {
		key := fmt.Sprintf("%v%v", KeywordPrefix, word)

		success := false

		// Retry in case of failures due to network or write conflicts
		for i := 0; i < DhtRetries; i++ {
			// The version in the DHT helps prevent write conflicts
			val, ver := dht.Get(m.dht, key)
			if val == nil {
				newval := make([]FileMetadata, 1)
				newval[0] = f
				success = dht.Put(m.dht, key, newval, 0)
			} else {
				if files, ok := val.([]FileMetadata); ok {
					files = append(files, f)
					success = dht.Put(m.dht, key, files, ver)
				} else {
					log.Fatalf("Invalid data in DHT. Expected File Metadata for key %v. Found %v", key, val)
				}
			}
			if success {
				break
			}
		}

		if !success {
			InfoLog("MELODY failed to write data to DHT")
		}
	}
}

func (m *Melody) LookupFiles(query string) []FileMetadata {
	keywords := strings.Fields(query)
	// TODO: We may want to add an upper bound to results.
	results := make([]FileMetadata, 0)

	for _, word := range keywords {
		key := fmt.Sprintf("%v%v", KeywordPrefix, word)
		val, _ := dht.Get(m.dht, key)

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

	success := false

	for i := 0; i < DhtRetries; i++ {
		// The version in the DHT helps prevent write conflicts
		val, ver := dht.Get(m.dht, key)

		if val == nil {
			newval := make([]string, 1)
			newval[0] = peerAddress
			seederInfo := FileSeederInfo{}
			seederInfo.Metadata = f
			seederInfo.Seeders = newval
			success = dht.Put(m.dht, key, seederInfo, 0)
		} else {
			if seederInfo, ok := val.(FileSeederInfo); ok {
				seederInfo.Seeders = append(seederInfo.Seeders, peerAddress)
				success = dht.Put(m.dht, key, seederInfo, ver)
			} else {
				log.Fatalf("Invalid data in DHT. Expected FileSeederInfo for key %v.", key)
			}
		}

		if success {
			break
		}
	}

	if !success {
		InfoLog("MELODY failed to write data to DHT")
	}
}

func (m *Melody) LocateSeeders(fileId string) FileSeederInfo {
	key := fmt.Sprintf("%v%v", FilePrefix, fileId)
	val, _ := dht.Get(m.dht, key)

	if val == nil {
		return FileSeederInfo{}
	}

	if seederInfo, ok := val.(FileSeederInfo); ok {
		return seederInfo
	} else {
		log.Fatalf("Invalid data in DHT. Expected FileSeederInfo for key %v.", key)
		return FileSeederInfo{}
	}
}
