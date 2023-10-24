package protocol

import (
	"fmt"
	"net"
	"runtime"
	"strings"
	"sync"

	"github.com/nsqio/nsq/internal/lg"
)

// 定义TCPHandler，只要实现了Handle(net.Conn)即认为是TCPHandler类型
// 和鸭子类型一样，用来实现多态
type TCPHandler interface {
	Handle(net.Conn)
}

func TCPServer(listener net.Listener, handler TCPHandler, logf lg.AppLogFunc) error {
	logf(lg.INFO, "TCP: listening on %s", listener.Addr())

	var wg sync.WaitGroup

	for {
		// 循环接受tcp连接
		clientConn, err := listener.Accept()
		if err != nil {
			// net.Error.Temporary() is deprecated, but is valid for accept
			// this is a hack to avoid a staticcheck error
			if te, ok := err.(interface{ Temporary() bool }); ok && te.Temporary() {
				logf(lg.WARN, "temporary Accept() failure - %s", err)
				runtime.Gosched()
				continue
			}
			// theres no direct way to detect this error because it is not exposed
			if !strings.Contains(err.Error(), "use of closed network connection") {
				return fmt.Errorf("listener.Accept() error - %s", err)
			}
			break
		}

		// 对每个连接调用handle函数创建协程进行处理, nsq中下面Handle对应tcp.go中的Handle方法
		wg.Add(1)
		go func() {
			handler.Handle(clientConn)
			wg.Done()
		}()
	}

	// wait to return until all handler goroutines complete
	wg.Wait()

	logf(lg.INFO, "TCP: closing %s", listener.Addr())

	return nil
}
