package utils
import (
	"fmt"
	"sync"
)

var mutex sync.Mutex

func Print(output ...string) {
	mutex.Lock()
	str := ""
	for _, s := range output {
		str = str + s + " "
	}
	fmt.Println(str)
	mutex.Unlock()
	return
}