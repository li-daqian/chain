package main

import (
	"bytes"
	"chain/serializer"
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

	ports                 = [2]string{client1, client2}
	collectChannel        = make(chan *collect)
	collectBatchPos int32 = 0
)

type collect struct {
	batchPos    int
	errorIdList []string
}

type finishData struct {
	batchPos int
}

func backendInit() {
	traceIdBatchList = make([]*TraceIdBatch, 0, bucketSize)
	traceCheckSumData = make(map[string]string)

	for i := 0; i < bucketSize; i++ {
		traceIdBatch := &TraceIdBatch{
			batchPos:     -1,
			processCount: 0,
			errorIdList:  make([]string, 0, 1024),
			mux:          sync.Mutex{},
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
		body, _ := ioutil.ReadAll(request.Body)
		var data finishData
		_ = json.Unmarshal(body, &data)
		log.Printf("Receive finish %d", data.batchPos)
		atomic.AddInt32(&finishProcessCount, 1)

		if finishProcessCount >= processCount {
			for i := range traceIdBatchList {
				if traceIdBatchList[i].batchPos != -1 {
					invokeCollect(traceIdBatchList[i].batchPos)
				}
			}
			for {
				if isFinish() {
					sendCheckSum()
					break
				}
			}
		}
	})

	go func() {
		for {
			collect := <-collectChannel
			collectWrongTrace(collect.errorIdList, collect.batchPos)
		}
	}()
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

func isFinish() bool {
	for i := range traceIdBatchList {
		if traceIdBatchList[i].batchPos != -1 {
			log.Printf("traceIdBatch not finish. %v", traceIdBatchList[i])
			return false
		}
	}

	return true
}

func setWrongTraceId(data UploadData) {
	batchPos := data.BatchPos
	if batchPos < 0 {
		return
	}

	traceIdBatch := traceIdBatchList[batchPos%bucketSize]
	traceIdBatch.batchPos = batchPos

	traceIdBatch.mux.Lock()
	traceIdBatch.processCount++
	traceIdBatch.errorIdList = append(traceIdBatch.errorIdList, data.Errors...)
	if batchPos >= 1 && traceIdBatch.processCount >= processCount {
		batchPos -= 1
		invokeCollect(batchPos)
	}
	traceIdBatch.mux.Unlock()
}

func invokeCollect(batchPos int) {
	collectData := traceIdBatchList[batchPos%bucketSize]
	errorTraceIds := make([]string, len(collectData.errorIdList))
	copy(errorTraceIds, collectData.errorIdList)
	collectChannel <- &collect{batchPos: batchPos, errorIdList: errorTraceIds}
	// reset
	traceIdBatchList[batchPos%bucketSize] = &TraceIdBatch{
		batchPos:     -1,
		processCount: 0,
		errorIdList:  make([]string, 0, 1024),
		mux:          sync.Mutex{},
	}
}

func collectWrongTrace(errorTraceIds []string, batchPos int) {
	data := make(map[string]map[string]bool)

	var wg sync.WaitGroup
	var mux sync.Mutex
	wg.Add(processCount)

	for i := range ports {
		port := ports[i]
		go func() {
			processData := getWrongTrace(errorTraceIds, port, batchPos)
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

	collectBatchPos++
}

type getWrongTraceStruct struct {
	TraceIdList []string
	BatchPos    int
}

func getWrongTrace(traceIdList []string, port string, batchPos int) map[string][]string {
	data, _ := json.Marshal(getWrongTraceStruct{traceIdList, batchPos})
	response, _ := http.Post("http://localhost:"+port+"/getWrongTrace", "application/json", bytes.NewBuffer(data))

	body, _ := ioutil.ReadAll(response.Body)
	res := serializer.GetWrongTrace(body)
	return res
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
