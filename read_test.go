package main

import (
	"encoding/json"
	"log"
	"testing"
)

func TestRead(t *testing.T) {
	bytes := []byte{127}
	log.Printf("%v", bytes)
	log.Println(len(bytes))
}

func Test(t *testing.T) {
	line := []byte("68da5a35225bfda3|1592840904831677|477b6d6d47d93656|7409617b830b736b|887|LogisticsCenter|db.AlertTemplateDao.searchByComplexByPage(..)|192.168.50.199|&component=java-spring-rest-template&span.kind=client&http.url=http://tracing.console.aliyun.com/getOrder?id=1&peer.port=9002&http.method=GET&http.status_code=403")
	log.Println(isError(line))
}

func TestMap(t *testing.T) {
	data := make(map[string][]string, 4096)
	for i := 0; i < 5; i++ {
		if _, ok := data["123"]; !ok {
			data["123"] = make([]string, 124)
		}
		data["123"] = append(data["123"], "12345")
	}


	log.Println(data)
}

func TestByteMap(t *testing.T) {
	data := make(map[string][][]byte)
	bytes := make([][]byte, 10)
	for i:=0; i<10; i++ {
		bytes = append(bytes, []byte("1234"+string(i)))
	}

	data["1"] = append(data["1"], bytes...)
	log.Println(data)

	jsonStr, _ := json.Marshal(data)
	log.Println(string(jsonStr))
	res := make(map[string][][]byte)
	_ = json.Unmarshal(jsonStr, res)
	log.Println(res)
}