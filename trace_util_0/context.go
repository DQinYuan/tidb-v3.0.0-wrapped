package trace_util_0

import (
	"encoding/hex"
	"encoding/json"
	"github.com/spaolacci/murmur3"
	"log"
	"net/http"
	"regexp"
	"strings"
	"sync"
)

var (
	mu         sync.RWMutex
	counterSet = make(map[string]*CoverCounter)
)

var (
	open   = false
	openMu sync.RWMutex
)

func digest(sql string) string {
	mur32 := murmur3.New32()
	mur32.Write([]byte(sql))
	encode := mur32.Sum(nil)
	return hex.EncodeToString(encode)
}

func Count(handle string, blockNum int) {
	mu.RLock()
	defer mu.RUnlock()

	for _, c := range counterSet {
		cover := c.m[handle]

		cover.mu.Lock()
		cover.Trace[[2]int{cover.Pos[blockNum*2], cover.Pos[blockNum*2+1]}] = struct{}{}
		cover.mu.Unlock()
	}
}

type SqlCover struct {
	Sql   string       `json:"sql"`
	Trace []*FileCover `json:"trace"`
}

type FileCover struct {
	File string   `json:"file"`
	Line [][2]int `json:"line"`
}

type OpenStatus struct {
	Before bool `json:"before"`
	After  bool `json:"after"`
}

func init() {
	go func() {
		handler := &RegexpHandler{}
		reg, _ := regexp.Compile(`/trace/*`)
		handler.HandleFunc(reg, func(writer http.ResponseWriter, request *http.Request) {
			writer.Header().Set("Access-Control-Allow-Origin", "*")
			writer.Header().Add("Access-Control-Allow-Headers", "Content-Type")
			writer.Header().Set("content-type", "application/json")

			digest := strings.TrimPrefix(request.URL.Path, "/trace/")

			mu.Lock()
			coverInfo := counterSet[digest]
			for kDigest := range counterSet {
				delete(counterSet, kDigest)
			}
			mu.Unlock()
			if coverInfo == nil {
				writer.WriteHeader(http.StatusNotFound)
				writer.Write([]byte("not fount"))
				return
			}

			sc := &SqlCover{Sql: coverInfo.sql}
			for fileName, cover := range coverInfo.m {
				fCover := &FileCover{File: fileName}
				cover.mu.RLock()
				for block := range cover.Trace {
					fCover.Line = append(fCover.Line, block)
				}
				cover.mu.RUnlock()
				sc.Trace = append(sc.Trace, fCover)
			}

			jsonBytes, err := json.Marshal(sc)
			if err != nil {
				log.Printf("Error: json Marshal %v\n", err)
			}

			writer.Write(jsonBytes)
		})

		reg, _ = regexp.Compile(`/status`)
		handler.HandleFunc(reg, func(writer http.ResponseWriter, request *http.Request) {
			// digest -> sql
			status := make(map[string]string, len(counterSet))
			mu.RLock()
			for digest, c := range counterSet {
				status[digest] = c.sql
			}
			mu.RUnlock()

			jsonBytes, err := json.Marshal(status)
			if err != nil {
				log.Printf("Error: json Marshal %v\n", err)
			}

			writer.Write(jsonBytes)
		})

		reg, _ = regexp.Compile(`/switch`)
		handler.HandleFunc(reg, func(writer http.ResponseWriter, request *http.Request) {
			openMu.Lock()
			oStatus := &OpenStatus{Before:open}
			open = !open
			oStatus.After = open
			openMu.Unlock()
			jsonBytes, err := json.Marshal(oStatus)
			if err != nil {
				log.Printf("Error: json Marshal %v\n", err)
			}

			writer.Write(jsonBytes)
		})

		http.ListenAndServe(":43222", handler)
	}()
}
