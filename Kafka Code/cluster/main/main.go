package main

import (
	p "Kafka/utils"
	b "Kafka/cluster/broker"
	z "Kafka/cluster/zookeeper"
	"sync"
	"time"
	"strconv"
	"net/http"
	"encoding/json"
	"io/ioutil"
	"bytes"
)

var wg sync.WaitGroup
var subscription = make(map[string][]int)

func Write_Topic(w http.ResponseWriter, r *http.Request) {
	var body, err = ioutil.ReadAll(r.Body)
	defer r.Body.Close()
	if err != nil {
		http.Error(w, err.Error(), 500)
		return
	}
	// Unmarshal
	var info b.Topic
	err = json.Unmarshal(body, &info)
	if err != nil {
		http.Error(w, err.Error(), 500)
		return
	}
	p.Print("Body from producer is", info.Topic_name, info.Message)
	status := b.Write_to_Topic(info)
	w.WriteHeader(http.StatusOK)
	w.Header().Set("Content-Type", "application/text")
	if status == "error" {
		w.Write([]byte("Error"))
	} else {
		w.Write([]byte("Success"))
		for i:=0; i<len(subscription[info.Topic_name]); i++ {
			port := subscription[info.Topic_name][i]
			url := "http://localhost:" + strconv.Itoa(port) + "/topic-update"
			req, err := http.NewRequest("POST", url, bytes.NewReader(body))
			if err != nil {
				p.Print("Impossible to build request: ", err.Error())
			}
			req.Close = true
			// add headers
			req.Header.Set("Content-Type", "application/json")
			// create http client
			// do not forget to set timeout; otherwise, no timeout!
			client := http.Client{Timeout: 10 * time.Second}
			// send the request
			res, err := client.Do(req)
			if err != nil {
				p.Print("Impossible to send request:", err.Error())
			}
			p.Print("Status Code:", strconv.Itoa(res.StatusCode))
			// we do not forget to close the body to free resources
			// defer will execute that at the end of the current function
			defer res.Body.Close()
			// read body
			resBody, err := ioutil.ReadAll(res.Body)
			if err != nil {
				p.Print("Impossible to read all body of response:", err.Error())
			} else {
				p.Print("Status of sending info to consumer on port ", strconv.Itoa(port), "is", string(resBody))
			}
		}
	}
	return
}

func Read_Topic(w http.ResponseWriter, r *http.Request) {
	body, err := ioutil.ReadAll(r.Body)
	defer r.Body.Close()
	if err != nil {
		http.Error(w, err.Error(), 500)
		return
	}
	// Unmarshal
	var info b.Subscribe_Message
	err = json.Unmarshal(body, &info)
	if err != nil {
		http.Error(w, err.Error(), 500)
		return
	}
	p.Print("Body of consumer is", info.Topic_name, strconv.FormatBool(info.Flag), strconv.Itoa(info.Port))
	// check if topic in subscription, else create it
	value, ok := subscription[info.Topic_name]
	var topic_content string = ""
	if ok {
		var found bool = false
		for i:=0; i < len(value); i++ { 
			if value[i] == info.Port {
				found = true
				break
			}
		}
		if found == false {
			subscription[info.Topic_name] = append(subscription[info.Topic_name], info.Port)
		}
	} else {
		subscription[info.Topic_name] = []int{}
		subscription[info.Topic_name] = append(subscription[info.Topic_name], info.Port)
	}
	//check if we need to fetch from start
	if info.Flag {
		topic_content, err = b.Read_from_Topic(info.Topic_name)
		if err != nil {
			p.Print("Error calling function Read_from_Topic")
		}
	}
	w.WriteHeader(http.StatusOK)
	w.Header().Set("Content-Type", "application/text")
	if err != nil {
		w.Write([]byte("Error fetching info"))
	} else {
		w.Write([]byte(topic_content))
	}
	return
}

func main() {
	var broker1 b.Broker_node
	broker1.Id = 1
	broker1.IsLeader = true
	broker1.Status = "alive"
	var p1 *b.Broker_node = &broker1
	var broker2 b.Broker_node
	broker2.Id = 2
	broker2.IsLeader = false
	broker2.Status = "alive"
	var p2 *b.Broker_node = &broker2
	var broker3 b.Broker_node
	broker3.Id = 3
	broker3.IsLeader = false
	broker3.Status = "alive"
	var p3 *b.Broker_node = &broker3
	mux := http.NewServeMux()
	mux.HandleFunc("/write", Write_Topic)
	mux.HandleFunc("/read", Read_Topic)
	srv := &http.Server{
        Addr:    ":8000",
        Handler: mux,
        WriteTimeout: 1 * time.Second,
    }
	wg.Add(4)
	go z.Zookeeper(p1, p2, p3)
	go b.Broker(p1)
	time.Sleep(1 * time.Second)
	go b.Broker(p2)
	time.Sleep(1 * time.Second)
	go b.Broker(p3)
	srv.ListenAndServe()
	wg.Wait()
}
