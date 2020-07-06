package main

import (
	"bufio"
	"bytes"
	"encoding/json"
	"io/ioutil"
	"log"
	"net/http"
	"sync"
)

var (
	bucketSize      = 64
	traceDataBucket = make([]map[string][]string, bucketSize)
	errorBucket     = make([][]string, bucketSize)
	lock            = sync.Mutex{}
	lockBucket      = make([]*sync.Cond, bucketSize)

	split    byte = '|'
	tagSplit byte = '&'

	error1       = "error=1"
	error1Length = len(error1)

	error2                 = "http.status_code="
	error2Length           = len(error2)
	httpStatusCodeOk       = "http.status_code=200"
	httpStatusCodeOKLength = len(httpStatusCodeOk)
)

func clientInit() {
	for i := 0; i < bucketSize; i++ {
		traceDataBucket[i] = make(map[string][]string, 4096)
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
	reader := bufio.NewReaderSize(resp.Body, 1024 * 512)
	for {
		lineByte, err := reader.ReadBytes('\n')
		if err != nil {
			break
		}
		line := string(lineByte[:len(lineByte)-1])
		count++

		traceId := ""
		for i := range line {
			if i >= 11 {
				if line[i] == split {
					traceId = line[:i]
					traceDataBucket[pos][traceId] = append(traceDataBucket[pos][traceId], line)
					break
				}
			}
		}

		if isError(line) {
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

func isError(line string) bool {
	tagLength := 0
	tagEndIndex := len(line)
	for i := tagEndIndex - 1; i >= 0; i-- {
		if line[i] == tagSplit || line[i] == split {

			if tagLength == error1Length {
				startIndex := i + 1
				j := startIndex
				for ; j < tagEndIndex; j++ {
					if line[j] != error1[j-startIndex] {
						break
					}
				}
				if j == tagEndIndex {
					return true
				}
			}

			if tagLength == httpStatusCodeOKLength {
				startIndex := i + 1
				j := startIndex
				for ; j < tagEndIndex; j++ {
					k := j - startIndex
					if k < error2Length {
						if line[j] != httpStatusCodeOk[k] {
							break
						}
					} else {
						if line[j] != httpStatusCodeOk[k] {
							return true
						}
					}
				}
				if j == tagEndIndex {
					return false
				}
			}

			tagLength = 0
			tagEndIndex = i

			if line[i] == split {
				break
			}
		} else {
			tagLength++
		}
	}

	return false
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
		getWrongTraceWithPos(prePos, wrongTraceIdList, wrongTraceData)
		getWrongTraceWithPos(pos, wrongTraceIdList, wrongTraceData)
		getWrongTraceWithPos(nextPos, wrongTraceIdList, wrongTraceData)
	}

	cond := lockBucket[prePos]
	cond.L.Lock()
	traceDataBucket[prePos] = make(map[string][]string, batchSize)
	errorBucket[prePos] = make([]string, 0, 512)
	cond.Broadcast()
	cond.L.Unlock()

	return wrongTraceData
}

func getWrongTraceWithPos(pos int, wrongTraceIdList []string, wrongTraceData map[string][]string) {
	traceData := traceDataBucket[pos]
	for i := range wrongTraceIdList {
		traceId := wrongTraceIdList[i]
		spanList := traceData[traceId]
		if spanList != nil {
			wrongTraceData[traceId] = append(wrongTraceData[traceId], spanList...)
		}
	}
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
