package broker

import (
	"sync"
	"time"
	"os"
	"io/ioutil"
	"errors"
	"strconv"
	p "Kafka/utils"
)

var Heartbeats = make(chan int, 100)
var wg sync.WaitGroup

type Broker_node struct {
	Id       int
	IsLeader bool
	Status   string
}

type Topic struct {
	Topic_name string
	Message string
}

type Subscribe_Message struct {
	Topic_name string
	Flag bool
	Port int
}

func Write_to_Topic(t Topic) string{
	// replace the path below with your system's path to topics folder
	path := "Path to topics"+t.Topic_name
	_, err := os.Stat(path)
	if err != nil {
		p.Print("Topic doesn't exist... Creating topic ")
		err := os.Mkdir(path, os.ModePerm)
		if err != nil {
			p.Print("Error in creating topic ", err.Error())
			return "error"
		}
	}
	filename := path+"/"+t.Topic_name+".txt"
	file, err := os.OpenFile(filename, os.O_RDWR|os.O_APPEND|os.O_CREATE, 0660)
    if err != nil {
        p.Print("Error opening file", err.Error())
        return "error"
    }
	defer file.Close()
	_, err = file.WriteString(t.Message+"\n")
	if err != nil {
        p.Print("Error writing/appending to file", err.Error())
        return "error"
    }
	return "success"
}

func Read_from_Topic(Topic_name string) (string, error) {
	// replace the path below with your system's path to topics folder
	path := "Path to topics"+Topic_name
	_, err := os.Stat(path)
	filename := path+"/"+Topic_name+".txt"
	if err != nil {
		p.Print("Topic doesn't exist... Creating topic ")
		err := os.Mkdir(path, os.ModePerm)
		if err != nil {
			p.Print("Error in creating topic directory ", err.Error())
			return "", errors.New("error")
		}
		_, err = os.Create(filename)
		if err != nil {
			p.Print("Error in creating topic file ", err.Error())
			return "", errors.New("error")
		}
		return "", nil
	}
	file_data, err := ioutil.ReadFile(filename)
    if err != nil {
        p.Print("Failed reading data from file: ", err.Error())
    }
	return string(file_data), nil
}

func Broker(b *Broker_node) {
	defer wg.Done()
	p.Print("Starting Broker", strconv.Itoa(b.Id))
	// to test election after failure
	// if b.Id == 1 {
	// 	Heartbeats <- b.Id
	// 	time.Sleep(20 * time.Second)
	// }
	for {
		if b.IsLeader {
			p.Print("Broker", strconv.Itoa(b.Id), "is leader")
		}
		if b.Status == "alive" {
			p.Print("Broker", strconv.Itoa(b.Id), "heartbeats received at", time.Now().String())
			Heartbeats <- b.Id
			time.Sleep(5 * time.Second)
		} else {
			p.Print("Failure")
		}
	}
}
