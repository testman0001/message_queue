package util

import (
	"sync"
)

type WaitGroupWrapper struct {
	sync.WaitGroup
}

// 封装cb，以协程方式并发执行，并加入等待组中
func (w *WaitGroupWrapper) Wrap(cb func()) {
	w.Add(1)
	go func() {
		cb()
		w.Done()
	}()
}
