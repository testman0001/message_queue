// 实际是最小堆实现的优先队列，优先级最高（prj最小）的消息放根部
package nsqd

type inFlightPqueue []*Message

func newInFlightPqueue(capacity int) inFlightPqueue {
	return make(inFlightPqueue, 0, capacity)
}

func (pq inFlightPqueue) Swap(i, j int) {
	pq[i], pq[j] = pq[j], pq[i]
	pq[i].index = i
	pq[j].index = j
}

func (pq *inFlightPqueue) Push(x *Message) {
	n := len(*pq)
	c := cap(*pq)
	if n+1 > c {
		// 扩容成两倍，不用手动释放旧的pq，go中对象没有任何引用时会被自动回收
		npq := make(inFlightPqueue, n, c*2)
		copy(npq, *pq)
		*pq = npq
	}
	*pq = (*pq)[0 : n+1]
	x.index = n
	// 存储到末尾，并上移至合适位置
	(*pq)[n] = x
	pq.up(n)
}

func (pq *inFlightPqueue) Pop() *Message {
	n := len(*pq)
	c := cap(*pq)
	// 将最高优先级消息交换到底部
	pq.Swap(0, n-1)
	// 交换上来的下移，n-1是为了不修改最后那个元素位置（即我们换下去那个最高优先级的消息）
	pq.down(0, n-1)
	if n < (c/2) && c > 25 {
		// 容量收缩至一半
		npq := make(inFlightPqueue, n, c/2)
		copy(npq, *pq)
		*pq = npq
	}
	x := (*pq)[n-1]
	x.index = -1
	*pq = (*pq)[0 : n-1]
	return x
}

func (pq *inFlightPqueue) Remove(i int) *Message {
	n := len(*pq)
	if n-1 != i {
		// 移到底部以移除
		pq.Swap(i, n-1)
		// 交换上来的下移
		pq.down(i, n-1)
		// 只要i的父节点不是n-1的祖先，那么小根堆中实际上i以上是可能比n-1的要更大的，需要额外进行上移
		pq.up(i)
	}
	x := (*pq)[n-1]
	x.index = -1
	*pq = (*pq)[0 : n-1]
	return x
}

// 获取优先级高于传参的消息，否则返回空
func (pq *inFlightPqueue) PeekAndShift(max int64) (*Message, int64) {
	if len(*pq) == 0 {
		return nil, 0
	}

	x := (*pq)[0]
	if x.pri > max {
		return nil, x.pri - max
	}
	pq.Pop()

	return x, 0
}

// 调整最小堆，上移到合适位置
func (pq *inFlightPqueue) up(j int) {
	for {
		// 二叉堆，根下标为0，可以直接计算出父节点下标
		i := (j - 1) / 2 // parent
		// 优先级低于父节点就不用继续上移了
		if i == j || (*pq)[j].pri >= (*pq)[i].pri {
			break
		}
		// 交换位置
		pq.Swap(i, j)
		j = i
	}
}

// 调整最小堆，下移到合适位置，n代表队列长度
func (pq *inFlightPqueue) down(i, n int) {
	for {
		j1 := 2*i + 1
		if j1 >= n || j1 < 0 { // j1 < 0 after int overflow
			break
		}
		j := j1 // left child
		// 关键点，在左右子树中找到较小的节点进行交换
		if j2 := j1 + 1; j2 < n && (*pq)[j1].pri >= (*pq)[j2].pri {
			j = j2 // = 2*i + 2  // right child
		}
		if (*pq)[j].pri >= (*pq)[i].pri {
			break
		}
		pq.Swap(i, j)
		i = j
	}
}
