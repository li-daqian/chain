package main

import (
	"bytes"
	"chain/serializer"
	"encoding/json"
	"io/ioutil"
	"log"
	"net/http"
	"sync"
)

const (
	bucketSize      = 128
	pageLength      = 1024 * 1024 * 16
	split      byte = '|'
)

var (
	pageBucket = [bucketSize]*Page{}
	lock       = sync.Mutex{}
	lockBucket = [bucketSize]*sync.Cond{}

	//readCost = int64(0)
	//findLineCost = int64(0)
	//findTraceIdCost = int64(0)
	//findErrorCost = int64(0)
)

func clientInit() {
	for i := 0; i < bucketSize; i++ {
		page := &Page{buffer: make([]byte, pageLength)}
		page.Reset()
		pageBucket[i] = page
		lockBucket[i] = sync.NewCond(&lock)
	}

	http.HandleFunc("/getWrongTrace", func(writer http.ResponseWriter, request *http.Request) {
		body, _ := ioutil.ReadAll(request.Body)

		var data getWrongTraceStruct
		_ = json.Unmarshal(body, &data)

		response := getWrongTracing(&data)
		_, _ = writer.Write(response)
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
	lineStartIndex := 0
ReadLoop:
	page := pageBucket[pos]
	for {
		//startTime := time.Now()
		read := page.Read(resp.Body)
		//readCost += time.Since(startTime).Nanoseconds()
		if read < 0 {
			goto Finish
		}
		buffer := page.buffer
		endIndex := read
		for {
			//startTime = time.Now()
			lineEndIndex := bytes.IndexByte(buffer[lineStartIndex:endIndex], '\n')
			//findLineCost += time.Since(startTime).Nanoseconds()
			if lineEndIndex < 0 {
				goto ReadLoop
			}
			count++
			lineEndIndex += lineStartIndex

			//startTime = time.Now()
			var traceId []byte
			for i := lineStartIndex + 11; i < lineStartIndex+lineEndIndex; i++ {
				if buffer[i] == split {
					traceId = buffer[lineStartIndex:i]
					page.putSpans(traceId, lineStartIndex, lineEndIndex)
					break
				}
			}
			//findTraceIdCost += time.Since(startTime).Nanoseconds()

			//startTime = time.Now()
			if isError(buffer[lineStartIndex:lineEndIndex]) {
				page.putErrors(traceId)
			}
			//findErrorCost = time.Since(startTime).Nanoseconds()

			if count%batchSize == 0 {
				//log.Printf("readCost: %d ms", readCost / 1000 / 1000)
				//log.Printf("findLineCost: %d ms", findLineCost / 1000 / 1000)
				//log.Printf("FindTraceIdCost: %d ms", findTraceIdCost / 1000 / 1000)
				//log.Printf("findErrorCost: %d ms", findErrorCost / 1000 / 1000)
				go uploadErrorTraceId((count/batchSize)-1, page.errors)

				pos++
				if pos >= bucketSize {
					pos = 0
				}

				if !pageBucket[pos].isEmpty {
					log.Printf("Read line block batchPos:%d", (count/batchSize)-1)
					cond := lockBucket[pos]
					cond.L.Lock()
					if !pageBucket[pos].isEmpty {
						cond.Wait()
					}
				}

				page.CopyNext(pageBucket[pos], lineStartIndex, endIndex)
				lineStartIndex = 0
				goto ReadLoop
			}

			lineStartIndex = lineEndIndex + 1
		}
	}

Finish:
	errorList := page.errors
	batchPos := (count/batchSize)-1
	if len(errorList) > 0 {
		uploadErrorTraceId(batchPos, errorList)
	}

	callFinish(batchPos)
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

func isError(line []byte) bool {
	searchStart := 100
	searchEnd := len(line) - 1
	for {
		pos := bytes.IndexByte(line[searchStart:searchEnd], '=')
		if pos < 0 {
			return false
		}
		searchStart += pos
		if line[searchStart-2] == 'd' && line[searchStart-1] == 'e' {
			return line[searchStart+1] != '2'
		}
		if line[searchStart-2] == 'o' && line[searchStart-1] == 'r' {
			return true
		}

		searchStart += 4
		if searchStart > searchEnd {
			return false
		}
	}
}

func getWrongTracing(data *getWrongTraceStruct) []byte {
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

	res := make([]byte, 0, 2048)
	if len(wrongTraceIdList) > 0 {
		serializer.PutType(&res, serializer.WrongTrace)
		for i := range wrongTraceIdList {
			traceId := wrongTraceIdList[i]
			getWrongTraceWithPos(prePos, traceId, &res)
			getWrongTraceWithPos(pos, traceId, &res)
			getWrongTraceWithPos(nextPos, traceId, &res)
		}
	}

	cond := lockBucket[prePos]
	cond.L.Lock()
	pageBucket[prePos].Reset()
	cond.Broadcast()
	cond.L.Unlock()

	return res
}

func getWrongTraceWithPos(pos int, traceId string, buffer *[]byte) {
	spanList := pageBucket[pos].getSpans([]byte(traceId))
	if spanList != nil {
		serializer.Put(buffer, []byte(traceId), spanList)
	}
}

type UploadData struct {
	BatchPos int
	Errors   []string
}

func uploadErrorTraceId(batchPos int, errorData [][]byte) {
	data := make([]string, 0, len(errorData))
	for i := range errorData {
		data = append(data, string(errorData[i]))
	}
	uploadData := UploadData{batchPos, data}
	jsonData, _ := json.Marshal(uploadData)
	_, _ = http.Post("http://localhost:"+backend+"/setWrongTraceId", "application/json", bytes.NewBuffer(jsonData))
}

func callFinish(batchPos int) {
	log.Println("Call finish")
	data := finishData{batchPos}
	jsonData, _ := json.Marshal(data)
	_, _ = http.Post("http://localhost:" + backend + "/finish", "application/json", bytes.NewBuffer(jsonData))
}
