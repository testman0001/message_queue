package main

import (
	"bufio"
	"flag"
	"log"
	"net"
	"sync"
	"sync/atomic"
	"time"

	"github.com/youzan/go-nsq"
)

var (
	runfor      = flag.Duration("runfor", 10*time.Second, "duration of time to run")
	sleepfor    = flag.Duration("sleepfor", 1*time.Second, " time to sleep between pub")
	keepAlive   = flag.Bool("keepalive", true, "keep alive for connection")
	tcpAddress  = flag.String("nsqd-tcp-address", "127.0.0.1:4150", "<addr>:<port> to connect to nsqd")
	topic       = flag.String("topic", "sub_bench", "topic to receive messages on")
	size        = flag.Int("size", 200, "size of messages")
	batchSize   = flag.Int("batch-size", 20, "batch size of messages")
	deadline    = flag.String("deadline", "", "deadline to start the benchmark run")
	concurrency = flag.Int("c", 100, "concurrency of goroutine")
)

var totalMsgCount int64
var currentMsgCount int64

func main() {
	flag.Parse()
	var wg sync.WaitGroup

	log.SetPrefix("[bench_writer] ")

	msg := make([]byte, *size)
	batch := make([][]byte, *batchSize)
	for i := range batch {
		batch[i] = msg
	}
	conn, err := net.DialTimeout("tcp", *tcpAddress, 5*time.Second)
	if err != nil {
		log.Println(err.Error())
	} else {
		conn.Write(nsq.MagicV2)
		nsq.CreateTopic(*topic, 0).WriteTo(conn)
		resp, err := nsq.ReadResponse(conn)
		if err != nil {
			log.Println(err.Error())
		} else {
			frameType, data, err := nsq.UnpackResponse(resp)
			if err != nil {
				log.Println(err.Error())
			} else if frameType == nsq.FrameTypeError {
				log.Println(string(data))
			}
		}
		conn.Close()
	}

	goChan := make(chan int)
	rdyChan := make(chan int)
	for j := 0; j < *concurrency; j++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			pubWorker(*runfor, *tcpAddress, *batchSize, batch, *topic, rdyChan, goChan)
		}()
		<-rdyChan
	}

	if *deadline != "" {
		t, err := time.Parse("2006-01-02 15:04:05", *deadline)
		if err != nil {
			log.Fatal(err)
		}
		d := t.Sub(time.Now())
		log.Printf("sleeping until %s (%s)", t, d)
		time.Sleep(d)
	}

	start := time.Now()
	close(goChan)
	go func() {
		prevMsgCount := int64(0)
		prevStart := start
		for {
			time.Sleep(time.Second * 5)
			end := time.Now()
			duration := end.Sub(prevStart)
			currentTmc := atomic.LoadInt64(&currentMsgCount)
			tmc := currentTmc - prevMsgCount
			prevMsgCount = currentTmc
			prevStart = time.Now()
			log.Printf("duration: %s - %.03fmb/s - %.03fops/s - %.03fus/op",
				duration,
				float64(tmc*int64(*size))/duration.Seconds()/1024/1024,
				float64(tmc)/duration.Seconds(),
				float64(duration/time.Microsecond)/(float64(tmc)+0.01))

		}

	}()
	wg.Wait()
	end := time.Now()
	duration := end.Sub(start)
	tmc := atomic.LoadInt64(&totalMsgCount)
	log.Printf("duration: %s - %.03fmb/s - %.03fops/s - %.03fus/op",
		duration,
		float64(tmc*int64(*size))/duration.Seconds()/1024/1024,
		float64(tmc)/duration.Seconds(),
		float64(duration/time.Microsecond)/(float64(tmc)+0.01))
}

func checkShouldClose(err error) bool {
	if err != nil {
		log.Printf("err: %v\n", err)
		return true
	}
	return false
}

func pubWorker(td time.Duration, tcpAddr string, batchSize int, batch [][]byte, topic string, rdyChan chan int, goChan chan int) {
	shouldClose := !*keepAlive
	conn, err := net.DialTimeout("tcp", tcpAddr, 5*time.Second)
	if err != nil {
		log.Println(err.Error())
		shouldClose = true
	} else {
		conn.Write(nsq.MagicV2)
	}
	rdyChan <- 1
	<-goChan
	var msgCount int64
	endTime := time.Now().Add(td)
	rw := bufio.NewReadWriter(bufio.NewReader(conn), bufio.NewWriter(conn))
	for {
		if time.Now().After(endTime) {
			break
		}
		if (*sleepfor).Nanoseconds() > int64(10000) {
			time.Sleep(*sleepfor)
		}
		if shouldClose || !*keepAlive {
			if conn != nil {
				conn.Close()
			}
			conn, err = net.DialTimeout("tcp", tcpAddr, 5*time.Second)
			shouldClose = checkShouldClose(err)
			if shouldClose {
				continue
			}
			_, err = conn.Write(nsq.MagicV2)
			shouldClose = checkShouldClose(err)
			if shouldClose {
				continue
			}
			rw = bufio.NewReadWriter(bufio.NewReader(conn), bufio.NewWriter(conn))
		}
		conn.SetReadDeadline(time.Now().Add(time.Second))
		cmd, _ := nsq.MultiPublish(topic, batch)
		_, err := cmd.WriteTo(rw)
		shouldClose = checkShouldClose(err)
		if shouldClose {
			continue
		}
		err = rw.Flush()
		shouldClose = checkShouldClose(err)
		if shouldClose {
			continue
		}
		resp, err := nsq.ReadResponse(rw)
		shouldClose = checkShouldClose(err)
		if shouldClose {
			continue
		}
		frameType, data, err := nsq.UnpackResponse(resp)
		shouldClose = checkShouldClose(err)
		if shouldClose {
			continue
		}
		conn.SetReadDeadline(time.Time{})
		if frameType == nsq.FrameTypeError {
			log.Println("frame unexpected:" + string(data))
			shouldClose = true
		}
		msgCount += int64(len(batch))
		if time.Now().After(endTime) {
			break
		}
		atomic.AddInt64(&currentMsgCount, int64(len(batch)))
	}
	atomic.AddInt64(&totalMsgCount, msgCount)
}
