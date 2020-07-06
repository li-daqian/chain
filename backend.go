package main

import (
	"bytes"
	"crypto/md5"
	"encoding/hex"
	"encoding/json"
	"io/ioutil"
	"log"
	"net/http"
	"net/url"
	"sort"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
)

type TraceIdBatch struct {
	batchPos     int
	processCount int32
	errorIdList  []string
	mux          sync.Mutex
}

var (
	finishProcessCount int32 = 0
	traceIdBatchList   []*TraceIdBatch
	traceCheckSumData  map[string]string

	canCollect = false
	collectPos = 0
)

func backendInit() {
	traceIdBatchList = make([]*TraceIdBatch, 0, bucketSize)
	traceCheckSumData = make(map[string]string)

	for i := 0; i < bucketSize; i++ {
		traceIdBatch := &TraceIdBatch{
			batchPos:     -1,
			processCount: 0,
			errorIdList:  make([]string, 0, 512),
		}
		traceIdBatchList = append(traceIdBatchList, traceIdBatch)
	}

	http.HandleFunc("/setWrongTraceId", func(writer http.ResponseWriter, request *http.Request) {
		body, _ := ioutil.ReadAll(request.Body)

		var data UploadData
		_ = json.Unmarshal(body, &data)
		setWrongTraceId(data)
	})

	http.HandleFunc("/finish", func(writer http.ResponseWriter, request *http.Request) {
		log.Println("Receive finish")
		atomic.AddInt32(&finishProcessCount, 1)
	})

	go backEndProcess()
}

func backEndProcess() {
	ports := []string{client1, client2}
	var traceIdBatch *TraceIdBatch
	for {
		if !canCollect {
			continue
		}
		traceIdBatch = getFinishedBatch()
		if traceIdBatch == nil {
			if isFinish() {
				if sendCheckSum() {
					break
				}
			}
			continue
		}

		errorIdList := removeDuplicateValues(traceIdBatch.errorIdList)
		data := make(map[string]map[string]bool)

		var wg sync.WaitGroup
		var mux sync.Mutex
		wg.Add(processCount)

		for i := range ports {
			port := ports[i]
			go func() {
				processData := getWrongTrace(errorIdList, port, traceIdBatch.batchPos)
				mux.Lock()
				for key, value := range processData {
					if _, ok := data[key]; !ok {
						data[key] = make(map[string]bool)
					}
					for i := range value {
						v := value[i]
						if _, ok := data[key][v]; !ok {
							data[key][v] = true
						}
					}
				}
				mux.Unlock()
				wg.Done()
			}()
		}
		wg.Wait()

		spanDataList := make([]string, 0, 256)
		for traceId, spanData := range data {
			for k, _ := range spanData {
				spanDataList = append(spanDataList, k)
			}
			sort.SliceStable(spanDataList, func(i, j int) bool {
				return getStartTime(spanDataList[i]) < getStartTime(spanDataList[j])
			})
			spans := strings.Join(spanDataList, "\n") + "\n"
			spansBytes := md5.Sum([]byte(spans))
			traceCheckSumData[traceId] = strings.ToUpper(hex.EncodeToString(spansBytes[:]))

			spanDataList = make([]string, 0, 256)
		}

		collectPos++
	}
}

func getStartTime(spanData string) int64 {
	startIndex := 0
	matchCount := 0
	for i := range spanData {
		if spanData[i] == split {
			if matchCount == 0 {
				startIndex = i + 1
			}
			if matchCount == 1 {
				startTime, _ := strconv.ParseInt(spanData[startIndex:i], 10, 64)
				return startTime
			}
			matchCount++
		}
	}
	return -1
}

func getFinishedBatch() *TraceIdBatch {
	current := collectPos % bucketSize
	next := current + 1
	if next >= bucketSize {
		next = 0
	}
	currentBucket := traceIdBatchList[current]
	nextBucket := traceIdBatchList[next]
	if finishProcessCount >= processCount && currentBucket.batchPos >= 0 ||
		nextBucket.processCount >= processCount && currentBucket.processCount >= processCount {
		// reset
		traceIdBatchList[current] = &TraceIdBatch{
			batchPos:     -1,
			processCount: 0,
			errorIdList:  make([]string, 0, 512),
		}
		return currentBucket
	}

	return nil
}

func isFinish() bool {
	if finishProcessCount < processCount {
		return false
	}

	for i := range traceIdBatchList {
		if traceIdBatchList[i].batchPos != -1 {
			return false
		}
	}

	return true
}

func setWrongTraceId(data UploadData) {
	if data.BatchPos < 0 {
		return
	}

	//log.Printf("SetWrongTraceId: %v", data)

	batchPos := data.BatchPos % bucketSize

	traceIdBatch := traceIdBatchList[batchPos]
	traceIdBatch.batchPos = data.BatchPos
	atomic.AddInt32(&traceIdBatch.processCount, 1)

	list := &traceIdBatch.errorIdList

	traceIdBatch.mux.Lock()
	*list = append(*list, data.Errors...)
	traceIdBatch.mux.Unlock()

	if batchPos == 1 && traceIdBatch.processCount >= 2 {
		canCollect = true
	}
}

type getWrongTraceStruct struct {
	TraceIdList []string
	BatchPos    int
}

func getWrongTrace(traceIdList []string, port string, batchPos int) map[string][]string {
	data, _ := json.Marshal(getWrongTraceStruct{traceIdList, batchPos})
	response, _ := http.Post("http://localhost:"+port+"/getWrongTrace", "application/json", bytes.NewBuffer(data))

	var result map[string][]string
	body, _ := ioutil.ReadAll(response.Body)
	_ = json.Unmarshal(body, &result)
	return result
}

func sendCheckSum() bool {
	result, _ := json.Marshal(traceCheckSumData)
	response, err := http.PostForm("http://localhost:"+dataSourcePort+"/api/finished", url.Values{
		"result": []string{string(result)},
	})
	if err != nil {
		return false
	}
	if response.StatusCode != 200 {
		return false
	}
	return true
}
