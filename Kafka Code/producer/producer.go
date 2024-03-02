package producer

import (
	"bufio"
	"strings"
	"os"
	"sync"
	"encoding/json"
	"net/http"
	"time"
	"bytes"
	"io"
	"strconv"
	b "Kafka/cluster/broker"
	p "Kafka/utils"
)

var wg sync.WaitGroup

func Producer() {
	defer wg.Done()
	var topic b.Topic
	scanner := bufio.NewScanner(os.Stdin)
	for {
		p.Print("Enter topic, message (press Ctrl+C to end):")
		scanner.Scan()
		// Get the current line of text
		text := scanner.Text()
		if text == "" {
			continue
		}
		topic.Topic_name, topic.Message, _ = strings.Cut(text, ", ")
		p.Print("Writing message", topic.Message, "into topic", topic.Topic_name)
		// convert topic data to JSON
		message_info, err := json.Marshal(topic)
		if err != nil {
			p.Print("impossible to marshall info:", err.Error())
		}
		req, err := http.NewRequest("POST", "http://localhost:8000/write", bytes.NewReader(message_info))
		if err != nil {
			p.Print("impossible to build request:", err.Error())
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
		p.Print("Status Code: ", strconv.Itoa(res.StatusCode))
		// we do not forget to close the body to free resources
		// defer will execute that at the end of the current function
		defer res.Body.Close()
		// read body
		resBody, err := io.ReadAll(res.Body)
		if err != nil {
			p.Print("Impossible to read all body of response: ", err.Error())
		}
		p.Print("res body: ", string(resBody))
		p.Print()
	}
}