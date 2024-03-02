package main

import (
	"sync"
	"net"
	"net/http"
	"io/ioutil"
	"encoding/json"
	"strconv"
	b "Kafka/cluster/broker"
	c "Kafka/consumer"
	p "Kafka/utils"
)

var wg sync.WaitGroup

func Print_Info(w http.ResponseWriter, r *http.Request) {
	body, err := ioutil.ReadAll(r.Body)
	defer r.Body.Close()
	var status string = "success"
	if err != nil {
		p.Print("Error reading body")
		status = "error"
	}
	var info b.Topic
	err = json.Unmarshal(body, &info)
	if err != nil {
		p.Print("Error unmarshalling body")
		status = "error"
	}
	p.Print("New information received:", info.Topic_name, info.Message, "\n")
	p.Print("Enter topic or topic --from beginning (press Ctrl+C to end):")
	w.WriteHeader(http.StatusOK)
	w.Header().Set("Content-Type", "application/text")
	w.Header().Set("Access-Control-Allow-Origin", "*")
	if status == "error" {
		w.Write([]byte("Error"))
	} else {
		w.Write([]byte("Success"))
	}
	return
}

func main() {
	wg.Add(1)

	listener, err := net.Listen("tcp", ":0")
    if err != nil {
        p.Print("Error encountered", err.Error())
    }
    defer listener.Close()

    // Get the actual port that was allocated
    port := listener.Addr().(*net.TCPAddr).Port
    p.Print("Server is running on port", strconv.Itoa(port), "\n")

	go c.Consumer_Subscribe(port)

	// Create a new HTTP server and register the handler
    http.HandleFunc("/topic-update", Print_Info)
    err = http.Serve(listener, nil)
    if err != nil {
        panic(err)
    }

	wg.Wait()
}