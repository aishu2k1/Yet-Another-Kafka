package consumer

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

func Consumer_Subscribe(port int) {
	defer wg.Done()
	scanner := bufio.NewScanner(os.Stdin)
	for {
		p.Print("Enter topic or topic --from beginning (press Ctrl+C to end):")
		scanner.Scan()
		// Get the current line of text
		text := scanner.Text()
		if text == "" {
			continue
		}
		// setting ID's default value as 0
		// it will be changed in the cluster
		info := b.Subscribe_Message{text, false, port}
		var topic_name string = text
		if strings.Contains(text, "--from beginning") {
			// strings.cut returns before, after, found 
			before, _, _ := strings.Cut(text, " --from beginning")
			topic_name = before
			info.Topic_name = topic_name
			info.Flag = true
		} 
		p.Print("Subscribed to topic", topic_name)
		// convert topic data to JSON
		message_info, err := json.Marshal(info)
		if err != nil {
			p.Print("Impossible to marshall info:", err.Error())
		}
		p.Print("Request sent to cluster\n")
		req, err := http.NewRequest("POST", "http://localhost:8000/read", bytes.NewReader(message_info))
		if err != nil {
			p.Print("Impossible to build request:", err.Error())
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
		p.Print("Status Code:", strconv.Itoa(res.StatusCode), "\n")
		// we do not forget to close the body to free resources
		// defer will execute that at the end of the current function
		defer res.Body.Close()
		// read body
		resBody, err := io.ReadAll(res.Body)
		if err != nil {
			p.Print("Impossible to read all body of response:", err.Error())
		}
		if len(resBody) > 0 {
			p.Print("Content of topic ", topic_name, " is:")
			var temp string = ""
			for i:=0; i<len(resBody); i++ {
				if resBody[i]!='\n' {
					temp = temp+string(resBody[i])
				} else {
					p.Print(temp)
					temp=""
				}
			}
		}
		p.Print("\n")
	}
}