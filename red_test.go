package main

import (
	"bytes"
	"log"
	"net/http"
	"testing"
)

func TestRead(t *testing.T) {
	pageLength := 1024*1024*4
	buffer := make([]byte, pageLength)

	tailLength := 0

	url := "http://localhost/trace1.data"
	resp, _ := http.Get(url)
	count := 0
	for {
		read, _ := resp.Body.Read(buffer[tailLength:pageLength-tailLength])
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
			isError(buffer[startIndex : startIndex+lineIndex])

			startIndex += lineIndex + 1
			if startIndex > endIndex {
				tailLength = 0
				break
			}
		}
	}
}

func Test(t *testing.T) {
	line := []byte("68da5a35225bfda3|1592840904831677|477b6d6d47d93656|7409617b830b736b|887|LogisticsCenter|db.AlertTemplateDao.searchByComplexByPage(..)|192.168.50.199|&component=java-spring-rest-template&span.kind=client&http.url=http://tracing.console.aliyun.com/getOrder?id=1&peer.port=9002&http.method=GET&http.status_code=403")
	log.Println(isError(line))
}

func TestMap(t *testing.T) {
	data := make(map[string][]string, 4096)
	for i := 0; i < 5; i++ {
		if _, ok := data["123"]; !ok {
			data["123"] = make([]string, 0, 124)
		}
		data["123"] = append(data["123"], "12345")
	}


	log.Println(data)
}