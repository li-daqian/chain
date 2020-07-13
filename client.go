package main

import (
	"bytes"
	"encoding/json"
	"io/ioutil"
	"log"
	"net/http"
	"sync"
)

const (
	bucketSize      = 64
	pageLength = 1024 * 1024 * 4

	split    byte = '|'
)
var (
	traceDataBucket = make([]map[string][]string, bucketSize)
	errorBucket     = make([][]string, bucketSize)
	lock            = sync.Mutex{}
	lockBucket      = make([]*sync.Cond, bucketSize)

	buffer     = make([]byte, pageLength)
)

func clientInit() {
	for i := 0; i < bucketSize; i++ {
		traceDataBucket[i] = make(map[string][]string, batchSize)
		errorBucket[i] = make([]string, 0, 512)
		lockBucket[i] = sync.NewCond(&lock)
	}

	http.HandleFunc("/getWrongTrace", func(writer http.ResponseWriter, request *http.Request) {
		body, _ := ioutil.ReadAll(request.Body)

		var data getWrongTraceStruct
		_ = json.Unmarshal(body, &data)

		response := getWrongTracing(&data)
		responseBytes, _ := json.Marshal(response)
		_, _ = writer.Write(responseBytes)
	})
}

func clientProcess() {
	url := getUrl()

	resp, err := http.Get(url)
	if err != nil {
		return
	}

	count := 0
	pos := 0
	tailLength := 0
	for {
		read, _ := resp.Body.Read(buffer[tailLength : pageLength-tailLength])
		if read == 0 {
			break
		}
		endIndex := read + tailLength

		startIndex := 0
		for {
			lineIndex := bytes.IndexByte(buffer[startIndex:endIndex], '\n')
			if lineIndex < 0 {
				tailLength = endIndex - startIndex
				copy(buffer[:tailLength], buffer[startIndex:endIndex])
				break
			}

			count++
			lineEndIndex := startIndex + lineIndex
			traceId := ""
			for i:= startIndex + 11; i < lineEndIndex; i++ {
				if buffer[i] == split {
					traceId = string(buffer[startIndex:i])
					//traceIdHash := hash(buffer, startIndex)
					traceDataBucket[pos][traceId] = append(traceDataBucket[pos][traceId], string(buffer[startIndex:lineEndIndex]))
					break
				}
			}

			if isError(buffer, startIndex, lineEndIndex) {
				errorBucket[pos] = append(errorBucket[pos], traceId)
			}

			if count%batchSize == 0 {
				errorList := errorBucket[pos]
				go uploadErrorTraceId((count/batchSize)-1, errorList)

				pos++

				if pos >= bucketSize {
					pos = 0
				}

				if len(traceDataBucket[pos]) > 0 {
					log.Println("Read line block")
					cond := lockBucket[pos]
					cond.L.Lock()
					if len(traceDataBucket[pos]) > 0 {
						cond.Wait()
					}
				}
			}

			startIndex += lineIndex + 1
			if startIndex > endIndex {
				tailLength = 0
				break
			}
		}
	}

	errorList := errorBucket[pos]
	if len(errorList) > 0 {
		uploadErrorTraceId((count/batchSize)-1, errorList)
	}

	callFinish()
}

func getUrl() string {
	if port == client1 {
		return "http://localhost:" + dataSourcePort + "/trace1.data"
	}
	if port == client2 {
		return "http://localhost:" + dataSourcePort + "/trace2.data"
	}
	return ""
}

func isError(buffer []byte, start int, end int) bool {
	searchStart := start + 100
	searchEnd := end
	for {
		pos := bytes.IndexByte(buffer[searchStart:searchEnd], '=')
		if pos < 0 {
			return false
		}
		searchStart += pos
		if buffer[searchStart-2] == 'd' && buffer[searchStart-1] == 'e' {
			return buffer[searchStart+1] != '2'
		}
		if buffer[searchStart-2] == 'o' && buffer[searchStart-1] == 'r' {
			return true
		}

		searchStart += 4
		if searchStart > searchEnd {
			return false
		}
	}
}

func getWrongTracing(data *getWrongTraceStruct) map[string][]string {
	wrongTraceIdList := data.TraceIdList
	pos := data.BatchPos % bucketSize
	prePos := pos - 1
	nextPos := pos + 1
	if prePos < 0 {
		prePos = bucketSize - 1
	}
	if nextPos >= bucketSize {
		nextPos = 0
	}

	wrongTraceData := make(map[string][]string)
	if len(wrongTraceIdList) > 0 {
		for i := range wrongTraceIdList {
			traceId := wrongTraceIdList[i]
			//traceIdHash := hash([]byte(traceId), 0)
			wrongTraceData[traceId] = append(wrongTraceData[traceId], traceDataBucket[prePos][traceId]...)
			wrongTraceData[traceId] = append(wrongTraceData[traceId], traceDataBucket[pos][traceId]...)
			wrongTraceData[traceId] = append(wrongTraceData[traceId], traceDataBucket[nextPos][traceId]...)
		}
	}

	cond := lockBucket[prePos]
	cond.L.Lock()
	traceDataBucket[prePos] = make(map[string][]string, batchSize)
	errorBucket[prePos] = make([]string, 0, 512)
	cond.Broadcast()
	cond.L.Unlock()

	return wrongTraceData
}

type UploadData struct {
	BatchPos int
	Errors   []string
}

func uploadErrorTraceId(batchPos int, errorData []string) {
	uploadData := UploadData{batchPos, errorData}
	data, _ := json.Marshal(uploadData)
	_, _ = http.Post("http://localhost:"+backend+"/setWrongTraceId", "application/json", bytes.NewBuffer(data))
}

func callFinish() {
	_, _ = http.Get("http://localhost:" + backend + "/finish")
}

func hash(traceId []byte, start int) int32 {
	return (int32(traceId[start]) +
			int32(traceId[start+1])<<8 +
			int32(traceId[start+2])<<16 +
			int32(traceId[start+3])<<24) % batchSize
}