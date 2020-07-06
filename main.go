package main

import (
	"fmt"
	"log"
	"net/http"
	"os"
)

const (
	client1 = "8000"
	client2 = "8001"
	backend = "8002"

	batchSize = 20000
	processCount = 2
)

var (
	port = os.Getenv("port")
	dataSourcePort = "80"
)

func main() {
	log.Printf("Start on %s", port)
	if port == client1 || port == client2 {
		clientInit()
	}

	if port == backend {
		backendInit()
	}

	http.HandleFunc("/ready", func(writer http.ResponseWriter, request *http.Request) {})
	http.HandleFunc("/setParameter", func(writer http.ResponseWriter, request *http.Request) {
		dataSourcePort = request.URL.Query().Get("port")
		if port == client1 || port == client2 {
			go clientProcess()
		}
	})

	err := http.ListenAndServe(":"+port, nil)
	if err != nil {
		fmt.Println(err)
		return
	}
}