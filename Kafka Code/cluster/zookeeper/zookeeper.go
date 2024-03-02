package zookeeper

import (
	"sync"
	"time"
	"strconv"
	b "Kafka/cluster/broker"
	p "Kafka/utils"
)

var Leader = 1
var LeaderFailure = false
var wg sync.WaitGroup

func Zookeeper(p1 *b.Broker_node, p2 *b.Broker_node, p3 *b.Broker_node) {
	defer wg.Done()
	p.Print("Starting Zookeeper")
	t := time.Now()
	times := [3]time.Time{t, t, t}
	for {
		if len(b.Heartbeats) > 0 {
			h := <-b.Heartbeats
			time_now := time.Now()
			times[h-1] = time_now
			for i := 0; i < 3; i++ {
				if time_now.Sub(times[i]).Seconds() > 10 {
					if Leader == i+1 {
						LeaderFailure = true
						if Leader == 1 {
							p1.IsLeader = false
						}
						if Leader == 2 {
							p2.IsLeader = false
						}
						if Leader == 3 {
							p3.IsLeader = false
						}
						break
					}
				}
			}
			if LeaderFailure {
				LeaderFailure = false
				if h == 1 {
					Leader = 1
					p1.IsLeader = true
				}
				if h == 2 {
					Leader = 2
					p2.IsLeader = true
				}
				if h == 3 {
					Leader = 3
					p3.IsLeader = true
				}
				p.Print("New Leader ", strconv.Itoa(h))
			}
		}
	}
}
