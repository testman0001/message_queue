// 协议层，实现服务器侧和客户端tcp直接交互的接口
package nsqd

import (
	"bytes"
	"encoding/binary"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"math/rand"
	"net"
	"sync/atomic"
	"time"
	"unsafe"

	"github.com/nsqio/nsq/internal/protocol"
	"github.com/nsqio/nsq/internal/version"
)

const (
	frameTypeResponse int32 = 0
	frameTypeError    int32 = 1
	frameTypeMessage  int32 = 2
)

var separatorBytes = []byte(" ")
var heartbeatBytes = []byte("_heartbeat_")
var okBytes = []byte("OK")

type protocolV2 struct {
	nsqd *NSQD
}

func (p *protocolV2) NewClient(conn net.Conn) protocol.Client {
	// 生成客户端id
	clientID := atomic.AddInt64(&p.nsqd.clientIDSequence, 1)
	// 创建v2类型客户端
	return newClientV2(clientID, conn, p.nsqd)
}

// 处理客户端tcp连接的核心接口
func (p *protocolV2) IOLoop(c protocol.Client) error {
	var err error
	var line []byte
	var zeroTime time.Time

	// 断言，失败panic，成功则赋值
	client := c.(*clientV2)

	// synchronize the startup of messagePump in order
	// to guarantee that it gets a chance to initialize
	// goroutine local state derived from client attributes
	// and avoid a potential race with IDENTIFY (where a client
	// could have changed or disabled said attributes)
	// 起一个消息投递协程，并等待启动完毕，避免启动期间收到客户端的identify命令导致属性错误
	messagePumpStartedChan := make(chan bool)
	go p.messagePump(client, messagePumpStartedChan)
	<-messagePumpStartedChan

	for {
		// 根据心跳间隔设置下次读取的最长时间
		if client.HeartbeatInterval > 0 {
			client.SetReadDeadline(time.Now().Add(client.HeartbeatInterval * 2))
		} else {
			client.SetReadDeadline(zeroTime)
		}

		// ReadSlice does not allocate new space for the data each request
		// ie. the returned slice is only valid until the next call to it
		// 读取命令，异常退出
		line, err = client.Reader.ReadSlice('\n')
		if err != nil {
			if err == io.EOF {
				err = nil
			} else {
				err = fmt.Errorf("failed to read command - %s", err)
			}
			break
		}

		// trim the '\n'
		line = line[:len(line)-1]
		// optionally trim the '\r'
		if len(line) > 0 && line[len(line)-1] == '\r' {
			line = line[:len(line)-1]
		}
		params := bytes.Split(line, separatorBytes)

		p.nsqd.logf(LOG_DEBUG, "PROTOCOL(V2): [%s] %s", client, params)

		var response []byte

		// 执行命令
		response, err = p.Exec(client, params)
		if err != nil {
			ctx := ""
			if parentErr := err.(protocol.ChildErr).Parent(); parentErr != nil {
				ctx = " - " + parentErr.Error()
			}
			p.nsqd.logf(LOG_ERROR, "[%s] - %s%s", client, err, ctx)

			sendErr := p.Send(client, frameTypeError, []byte(err.Error()))
			if sendErr != nil {
				p.nsqd.logf(LOG_ERROR, "[%s] - %s%s", client, sendErr, ctx)
				break
			}

			// errors of type FatalClientErr should forceably close the connection
			if _, ok := err.(*protocol.FatalClientErr); ok {
				break
			}
			continue
		}

		// 响应结果
		if response != nil {
			err = p.Send(client, frameTypeResponse, response)
			if err != nil {
				err = fmt.Errorf("failed to send response - %s", err)
				break
			}
		}
	}

	// 关闭客户端连接
	p.nsqd.logf(LOG_INFO, "PROTOCOL(V2): [%s] exiting ioloop", client)
	close(client.ExitChan)
	if client.Channel != nil {
		client.Channel.RemoveClient(client.ID)
	}

	return err
}

// 投递消息给客户端
func (p *protocolV2) SendMessage(client *clientV2, msg *Message) error {
	p.nsqd.logf(LOG_DEBUG, "PROTOCOL(V2): writing msg(%s) to client(%s) - %s", msg.ID, client, msg.Body)

	// 缓冲区在消息发送完后释放
	buf := bufferPoolGet()
	defer bufferPoolPut(buf)

	_, err := msg.WriteTo(buf)
	if err != nil {
		return err
	}

	err = p.Send(client, frameTypeMessage, buf.Bytes())
	if err != nil {
		return err
	}

	return nil
}

// 数据发送接口
func (p *protocolV2) Send(client *clientV2, frameType int32, data []byte) error {
	client.writeLock.Lock()

	// 设置写超时的绝对时间
	var zeroTime time.Time
	if client.HeartbeatInterval > 0 {
		client.SetWriteDeadline(time.Now().Add(client.HeartbeatInterval))
	} else {
		client.SetWriteDeadline(zeroTime)
	}

	// 使用bufio库, 数据先写到缓冲区
	_, err := protocol.SendFramedResponse(client.Writer, frameType, data)
	if err != nil {
		client.writeLock.Unlock()
		return err
	}

	// 如果不是消息类型则直接清空缓冲区, 强制发送出去
	// 目的是减少系统调用次数, 提升效率
	if frameType != frameTypeMessage {
		err = client.Flush()
	}

	client.writeLock.Unlock()

	return err
}

func (p *protocolV2) Exec(client *clientV2, params [][]byte) ([]byte, error) {
	if bytes.Equal(params[0], []byte("IDENTIFY")) {
		return p.IDENTIFY(client, params)
	}
	err := enforceTLSPolicy(client, p, params[0])
	if err != nil {
		return nil, err
	}
	switch {
	case bytes.Equal(params[0], []byte("FIN")):
		return p.FIN(client, params)
	// 告知服务器自身消费能力
	case bytes.Equal(params[0], []byte("RDY")):
		return p.RDY(client, params)
	// 让消息重新入队
	case bytes.Equal(params[0], []byte("REQ")):
		return p.REQ(client, params)
	case bytes.Equal(params[0], []byte("PUB")):
		return p.PUB(client, params)
	case bytes.Equal(params[0], []byte("MPUB")):
		return p.MPUB(client, params)
	// 发布延迟消息
	case bytes.Equal(params[0], []byte("DPUB")):
		return p.DPUB(client, params)
	// 空操作
	case bytes.Equal(params[0], []byte("NOP")):
		return p.NOP(client, params)
	// 重置消费中消息的超时设置
	case bytes.Equal(params[0], []byte("TOUCH")):
		return p.TOUCH(client, params)
	case bytes.Equal(params[0], []byte("SUB")):
		return p.SUB(client, params)
	// 关闭连接
	case bytes.Equal(params[0], []byte("CLS")):
		return p.CLS(client, params)
	case bytes.Equal(params[0], []byte("AUTH")):
		return p.AUTH(client, params)
	}
	return nil, protocol.NewFatalClientErr(nil, "E_INVALID", fmt.Sprintf("invalid command %s", params[0]))
}

// 核心接口, 用来把channel中的消息投递给客户端
func (p *protocolV2) messagePump(client *clientV2, startedChan chan bool) {
	var err error
	// 监听内存消息通道
	var memoryMsgChan chan *Message
	// 监听后台消息通道，该通道为可读属性，backendQueue存放已发送但未确认消息，负责ack机制并持久化到磁盘，默认一个topic一个
	var backendMsgChan <-chan []byte
	// 客户端订阅的channel对象，在sub命令触发后写入client.SubEventChan，从该通道读出channel
	var subChannel *Channel
	// NOTE: `flusherChan` is used to bound message latency for
	// the pathological case of a channel on a low volume topic
	// with >1 clients having >1 RDY counts
	// 该通道为可读属性
	var flusherChan <-chan time.Time
	var sampleRate int32

	// 订阅事件通道，sub命令触发写入
	subEventChan := client.SubEventChan
	// 认证事件通道, identify命令触发写入，客户端与服务器以此进行参数协商
	identifyEventChan := client.IdentifyEventChan
	// 定时器, 定时清空buffer，发送消息, 通过identify协商, 默认应该是250ms, 通过该项决定写客户端的频率
	outputBufferTicker := time.NewTicker(client.OutputBufferTimeout)
	// 心跳超时定时器
	heartbeatTicker := time.NewTicker(client.HeartbeatInterval)
	heartbeatChan := heartbeatTicker.C
	// 消息发送后到收到消费确认的超时时间，超过时重新入队，默认1min
	msgTimeout := client.MsgTimeout

	// v2 opportunistically buffers data to clients to reduce write system calls
	// we force flush in two cases:
	//    1. when the client is not ready to receive messages
	//    2. we're buffered and the channel has nothing left to send us
	//       (ie. we would block in this loop anyway)
	//
	flushed := true

	// signal to the goroutine that started the messagePump
	// that we've started up
	close(startedChan)

	for {
		if subChannel == nil || !client.IsReadyForMessages() {
			// the client is not ready to receive messages...
			// 上面这条官方注释就是狗屁, 实际上IsReadyForMessages是判断消息还能不能继续发给客户端, 会不会命名啊???
			// 大部分场景是在连续投消息使得处理中消息到达阈值后进入此处逻辑, 因为消息都是放缓冲区没实际发送, 此时需要一次性发送出去
			memoryMsgChan = nil
			backendMsgChan = nil
			flusherChan = nil
			// force flush
			client.writeLock.Lock()
			err = client.Flush()
			client.writeLock.Unlock()
			if err != nil {
				goto exit
			}
			flushed = true
		} else if flushed {
			// last iteration we flushed...
			// do not select on the flusher ticker channel
			// 消息可能直接投到channel的内存消息通道，或者满了时投到后台队列（非临时channel即磁盘队列）
			memoryMsgChan = subChannel.memoryMsgChan
			// 后台队列暴露一个只读的通道，可容纳byte数组（多条消息）
			backendMsgChan = subChannel.backend.ReadChan()
			// 上一次循环清空过消息buffer, 这次先屏蔽该信号通道
			flusherChan = nil
		} else {
			// we're buffered (if there isn't any more data we should flush)...
			// select on the flusher ticker channel, too
			// 客户端可接收消息，且非刷新状态，更新刷新计时器
			memoryMsgChan = subChannel.memoryMsgChan
			backendMsgChan = subChannel.backend.ReadChan()
			flusherChan = outputBufferTicker.C
		}

		select {
		case <-flusherChan:
			// if this case wins, we're either starved
			// or we won the race between other channels...
			// in either case, force flush
			// 定时器触发，进行实际发送
			client.writeLock.Lock()
			err = client.Flush()
			client.writeLock.Unlock()
			if err != nil {
				goto exit
			}
			flushed = true
		case <-client.ReadyStateChan:
			// channel状态发生改变，跳到循环开头进行处理
		case subChannel = <-subEventChan:
			// you can't SUB anymore
			// 从这里看来，一个消费者是不支持订阅多个channel的，而且订阅后无法更改
			subEventChan = nil
		case identifyData := <-identifyEventChan:
			// you can't IDENTIFY anymore
			// 根据协商信息进行更新
			identifyEventChan = nil

			outputBufferTicker.Stop()
			if identifyData.OutputBufferTimeout > 0 {
				outputBufferTicker = time.NewTicker(identifyData.OutputBufferTimeout)
			}

			heartbeatTicker.Stop()
			heartbeatChan = nil
			if identifyData.HeartbeatInterval > 0 {
				heartbeatTicker = time.NewTicker(identifyData.HeartbeatInterval)
				heartbeatChan = heartbeatTicker.C
			}

			if identifyData.SampleRate > 0 {
				sampleRate = identifyData.SampleRate
			}

			msgTimeout = identifyData.MsgTimeout
		case <-heartbeatChan:
			// 给客户端发心跳
			err = p.Send(client, frameTypeResponse, heartbeatBytes)
			if err != nil {
				goto exit
			}
		case b := <-backendMsgChan:
			// 从后台队列中取出消息处理
			if sampleRate > 0 && rand.Int31n(100) > sampleRate {
				continue
			}

			// 先解码
			msg, err := decodeMessage(b)
			if err != nil {
				p.nsqd.logf(LOG_ERROR, "failed to decode message - %s", err)
				continue
			}
			msg.Attempts++

			// 设置消息超时时间
			subChannel.StartInFlightTimeout(msg, client.ID, msgTimeout)
			// 开始发送
			// 计数器更新
			client.SendingMessage()
			// 实际发送接口，实际只是利用bufio写到缓冲区
			err = p.SendMessage(client, msg)
			if err != nil {
				goto exit
			}
			flushed = false
		case msg := <-memoryMsgChan:
			// 从内存消息通道取出消息处理，除了不要解码，其他和后台队列都是一致的
			if sampleRate > 0 && rand.Int31n(100) > sampleRate {
				continue
			}
			msg.Attempts++

			subChannel.StartInFlightTimeout(msg, client.ID, msgTimeout)
			client.SendingMessage()
			err = p.SendMessage(client, msg)
			if err != nil {
				goto exit
			}
			flushed = false
		case <-client.ExitChan:
			goto exit
		}
	}

exit:
	p.nsqd.logf(LOG_INFO, "PROTOCOL(V2): [%s] exiting messagePump", client)
	heartbeatTicker.Stop()
	outputBufferTicker.Stop()
	if err != nil {
		p.nsqd.logf(LOG_ERROR, "PROTOCOL(V2): [%s] messagePump error - %s", client, err)
	}
}

// 参数协商
func (p *protocolV2) IDENTIFY(client *clientV2, params [][]byte) ([]byte, error) {
	var err error

	if atomic.LoadInt32(&client.State) != stateInit {
		return nil, protocol.NewFatalClientErr(nil, "E_INVALID", "cannot IDENTIFY in current state")
	}

	bodyLen, err := readLen(client.Reader, client.lenSlice)
	if err != nil {
		return nil, protocol.NewFatalClientErr(err, "E_BAD_BODY", "IDENTIFY failed to read body size")
	}

	if int64(bodyLen) > p.nsqd.getOpts().MaxBodySize {
		return nil, protocol.NewFatalClientErr(nil, "E_BAD_BODY",
			fmt.Sprintf("IDENTIFY body too big %d > %d", bodyLen, p.nsqd.getOpts().MaxBodySize))
	}

	if bodyLen <= 0 {
		return nil, protocol.NewFatalClientErr(nil, "E_BAD_BODY",
			fmt.Sprintf("IDENTIFY invalid body size %d", bodyLen))
	}

	body := make([]byte, bodyLen)
	_, err = io.ReadFull(client.Reader, body)
	if err != nil {
		return nil, protocol.NewFatalClientErr(err, "E_BAD_BODY", "IDENTIFY failed to read body")
	}

	// body is a json structure with producer information
	var identifyData identifyDataV2
	err = json.Unmarshal(body, &identifyData)
	if err != nil {
		return nil, protocol.NewFatalClientErr(err, "E_BAD_BODY", "IDENTIFY failed to decode JSON body")
	}

	p.nsqd.logf(LOG_DEBUG, "PROTOCOL(V2): [%s] %+v", client, identifyData)

	err = client.Identify(identifyData)
	if err != nil {
		return nil, protocol.NewFatalClientErr(err, "E_BAD_BODY", "IDENTIFY "+err.Error())
	}

	// bail out early if we're not negotiating features
	if !identifyData.FeatureNegotiation {
		return okBytes, nil
	}

	tlsv1 := p.nsqd.tlsConfig != nil && identifyData.TLSv1
	deflate := p.nsqd.getOpts().DeflateEnabled && identifyData.Deflate
	deflateLevel := 6
	if deflate && identifyData.DeflateLevel > 0 {
		deflateLevel = identifyData.DeflateLevel
	}
	if max := p.nsqd.getOpts().MaxDeflateLevel; max < deflateLevel {
		deflateLevel = max
	}
	snappy := p.nsqd.getOpts().SnappyEnabled && identifyData.Snappy

	if deflate && snappy {
		return nil, protocol.NewFatalClientErr(nil, "E_IDENTIFY_FAILED", "cannot enable both deflate and snappy compression")
	}

	resp, err := json.Marshal(struct {
		MaxRdyCount         int64  `json:"max_rdy_count"`
		Version             string `json:"version"`
		MaxMsgTimeout       int64  `json:"max_msg_timeout"`
		MsgTimeout          int64  `json:"msg_timeout"`
		TLSv1               bool   `json:"tls_v1"`
		Deflate             bool   `json:"deflate"`
		DeflateLevel        int    `json:"deflate_level"`
		MaxDeflateLevel     int    `json:"max_deflate_level"`
		Snappy              bool   `json:"snappy"`
		SampleRate          int32  `json:"sample_rate"`
		AuthRequired        bool   `json:"auth_required"`
		OutputBufferSize    int    `json:"output_buffer_size"`
		OutputBufferTimeout int64  `json:"output_buffer_timeout"`
	}{
		MaxRdyCount:         p.nsqd.getOpts().MaxRdyCount,
		Version:             version.Binary,
		MaxMsgTimeout:       int64(p.nsqd.getOpts().MaxMsgTimeout / time.Millisecond),
		MsgTimeout:          int64(client.MsgTimeout / time.Millisecond),
		TLSv1:               tlsv1,
		Deflate:             deflate,
		DeflateLevel:        deflateLevel,
		MaxDeflateLevel:     p.nsqd.getOpts().MaxDeflateLevel,
		Snappy:              snappy,
		SampleRate:          client.SampleRate,
		AuthRequired:        p.nsqd.IsAuthEnabled(),
		OutputBufferSize:    client.OutputBufferSize,
		OutputBufferTimeout: int64(client.OutputBufferTimeout / time.Millisecond),
	})
	if err != nil {
		return nil, protocol.NewFatalClientErr(err, "E_IDENTIFY_FAILED", "IDENTIFY failed "+err.Error())
	}

	err = p.Send(client, frameTypeResponse, resp)
	if err != nil {
		return nil, protocol.NewFatalClientErr(err, "E_IDENTIFY_FAILED", "IDENTIFY failed "+err.Error())
	}

	if tlsv1 {
		p.nsqd.logf(LOG_INFO, "PROTOCOL(V2): [%s] upgrading connection to TLS", client)
		err = client.UpgradeTLS()
		if err != nil {
			return nil, protocol.NewFatalClientErr(err, "E_IDENTIFY_FAILED", "IDENTIFY failed "+err.Error())
		}

		err = p.Send(client, frameTypeResponse, okBytes)
		if err != nil {
			return nil, protocol.NewFatalClientErr(err, "E_IDENTIFY_FAILED", "IDENTIFY failed "+err.Error())
		}
	}

	if snappy {
		p.nsqd.logf(LOG_INFO, "PROTOCOL(V2): [%s] upgrading connection to snappy", client)
		err = client.UpgradeSnappy()
		if err != nil {
			return nil, protocol.NewFatalClientErr(err, "E_IDENTIFY_FAILED", "IDENTIFY failed "+err.Error())
		}

		err = p.Send(client, frameTypeResponse, okBytes)
		if err != nil {
			return nil, protocol.NewFatalClientErr(err, "E_IDENTIFY_FAILED", "IDENTIFY failed "+err.Error())
		}
	}

	// 更新压缩级别, 使用flate进行网络数据传输
	if deflate {
		p.nsqd.logf(LOG_INFO, "PROTOCOL(V2): [%s] upgrading connection to deflate (level %d)", client, deflateLevel)
		err = client.UpgradeDeflate(deflateLevel)
		if err != nil {
			return nil, protocol.NewFatalClientErr(err, "E_IDENTIFY_FAILED", "IDENTIFY failed "+err.Error())
		}

		err = p.Send(client, frameTypeResponse, okBytes)
		if err != nil {
			return nil, protocol.NewFatalClientErr(err, "E_IDENTIFY_FAILED", "IDENTIFY failed "+err.Error())
		}
	}

	return nil, nil
}

// 身份认证
func (p *protocolV2) AUTH(client *clientV2, params [][]byte) ([]byte, error) {
	if atomic.LoadInt32(&client.State) != stateInit {
		return nil, protocol.NewFatalClientErr(nil, "E_INVALID", "cannot AUTH in current state")
	}

	if len(params) != 1 {
		return nil, protocol.NewFatalClientErr(nil, "E_INVALID", "AUTH invalid number of parameters")
	}

	bodyLen, err := readLen(client.Reader, client.lenSlice)
	if err != nil {
		return nil, protocol.NewFatalClientErr(err, "E_BAD_BODY", "AUTH failed to read body size")
	}

	if int64(bodyLen) > p.nsqd.getOpts().MaxBodySize {
		return nil, protocol.NewFatalClientErr(nil, "E_BAD_BODY",
			fmt.Sprintf("AUTH body too big %d > %d", bodyLen, p.nsqd.getOpts().MaxBodySize))
	}

	if bodyLen <= 0 {
		return nil, protocol.NewFatalClientErr(nil, "E_BAD_BODY",
			fmt.Sprintf("AUTH invalid body size %d", bodyLen))
	}

	body := make([]byte, bodyLen)
	_, err = io.ReadFull(client.Reader, body)
	if err != nil {
		return nil, protocol.NewFatalClientErr(err, "E_BAD_BODY", "AUTH failed to read body")
	}

	if client.HasAuthorizations() {
		return nil, protocol.NewFatalClientErr(nil, "E_INVALID", "AUTH already set")
	}

	if !client.nsqd.IsAuthEnabled() {
		return nil, protocol.NewFatalClientErr(err, "E_AUTH_DISABLED", "AUTH disabled")
	}

	if err := client.Auth(string(body)); err != nil {
		// we don't want to leak errors contacting the auth server to untrusted clients
		p.nsqd.logf(LOG_WARN, "PROTOCOL(V2): [%s] AUTH failed %s", client, err)
		return nil, protocol.NewFatalClientErr(err, "E_AUTH_FAILED", "AUTH failed")
	}

	if !client.HasAuthorizations() {
		return nil, protocol.NewFatalClientErr(nil, "E_UNAUTHORIZED", "AUTH no authorizations found")
	}

	resp, err := json.Marshal(struct {
		Identity        string `json:"identity"`
		IdentityURL     string `json:"identity_url"`
		PermissionCount int    `json:"permission_count"`
	}{
		Identity:        client.AuthState.Identity,
		IdentityURL:     client.AuthState.IdentityURL,
		PermissionCount: len(client.AuthState.Authorizations),
	})
	if err != nil {
		return nil, protocol.NewFatalClientErr(err, "E_AUTH_ERROR", "AUTH error "+err.Error())
	}

	err = p.Send(client, frameTypeResponse, resp)
	if err != nil {
		return nil, protocol.NewFatalClientErr(err, "E_AUTH_ERROR", "AUTH error "+err.Error())
	}

	return nil, nil

}

// 权限检查
func (p *protocolV2) CheckAuth(client *clientV2, cmd, topicName, channelName string) error {
	// if auth is enabled, the client must have authorized already
	// compare topic/channel against cached authorization data (refetching if expired)
	if client.nsqd.IsAuthEnabled() {
		if !client.HasAuthorizations() {
			return protocol.NewFatalClientErr(nil, "E_AUTH_FIRST",
				fmt.Sprintf("AUTH required before %s", cmd))
		}
		ok, err := client.IsAuthorized(topicName, channelName)
		if err != nil {
			// we don't want to leak errors contacting the auth server to untrusted clients
			p.nsqd.logf(LOG_WARN, "PROTOCOL(V2): [%s] AUTH failed %s", client, err)
			return protocol.NewFatalClientErr(nil, "E_AUTH_FAILED", "AUTH failed")
		}
		if !ok {
			return protocol.NewFatalClientErr(nil, "E_UNAUTHORIZED",
				fmt.Sprintf("AUTH failed for %s on %q %q", cmd, topicName, channelName))
		}
	}
	return nil
}

// 订阅topic, 如果channel不存在则创建
func (p *protocolV2) SUB(client *clientV2, params [][]byte) ([]byte, error) {
	if atomic.LoadInt32(&client.State) != stateInit {
		return nil, protocol.NewFatalClientErr(nil, "E_INVALID", "cannot SUB in current state")
	}

	if client.HeartbeatInterval <= 0 {
		return nil, protocol.NewFatalClientErr(nil, "E_INVALID", "cannot SUB with heartbeats disabled")
	}

	if len(params) < 3 {
		return nil, protocol.NewFatalClientErr(nil, "E_INVALID", "SUB insufficient number of parameters")
	}

	topicName := string(params[1])
	if !protocol.IsValidTopicName(topicName) {
		return nil, protocol.NewFatalClientErr(nil, "E_BAD_TOPIC",
			fmt.Sprintf("SUB topic name %q is not valid", topicName))
	}

	channelName := string(params[2])
	if !protocol.IsValidChannelName(channelName) {
		return nil, protocol.NewFatalClientErr(nil, "E_BAD_CHANNEL",
			fmt.Sprintf("SUB channel name %q is not valid", channelName))
	}

	if err := p.CheckAuth(client, "SUB", topicName, channelName); err != nil {
		return nil, err
	}

	// This retry-loop is a work-around for a race condition, where the
	// last client can leave the channel between GetChannel() and AddClient().
	// Avoid adding a client to an ephemeral channel / topic which has started exiting.
	// 这里实现得很别扭，如果是临时channel，最后一个client在GetChannel()和AddClient()之间退出，导致channel整个触发exit，
	// 那么sub算是失败了，通过等退出结束，重新创建channel并订阅一次来避免，那等待期间又有client创建channel并退出就直接认为sub失败了
	// 个人感觉没啥意思，只是多重试了一次罢了
	var channel *Channel
	for i := 1; ; i++ {
		topic := p.nsqd.GetTopic(topicName)
		// 获取，获取不到就先创建
		channel = topic.GetChannel(channelName)
		if err := channel.AddClient(client.ID, client); err != nil {
			return nil, protocol.NewFatalClientErr(err, "E_SUB_FAILED", "SUB failed "+err.Error())
		}

		// 如果channel或topic是临时的，那么检查下是否在退出中，是则移除掉客户端，再试一次，第二轮如果又进来了，直接返回失败
		if (channel.ephemeral && channel.Exiting()) || (topic.ephemeral && topic.Exiting()) {
			// 注意，如果客户端数目为0，那么channel会自动删除；如果channel为0了，那么topic也会自动删除。类似rabbitmq里的ad队列
			channel.RemoveClient(client.ID)
			if i < 2 {
				time.Sleep(100 * time.Millisecond)
				continue
			}
			return nil, protocol.NewFatalClientErr(nil, "E_SUB_FAILED", "SUB failed to deleted topic/channel")
		}
		break
	}
	atomic.StoreInt32(&client.State, stateSubscribed)
	client.Channel = channel
	// update message pump
	client.SubEventChan <- channel

	return okBytes, nil
}

// 客户端(消费者)告知服务器自身消费能力
// MaxRdyCount影响到消息量很大时推送到客户端的频率
func (p *protocolV2) RDY(client *clientV2, params [][]byte) ([]byte, error) {
	state := atomic.LoadInt32(&client.State)

	if state == stateClosing {
		// just ignore ready changes on a closing channel
		p.nsqd.logf(LOG_INFO,
			"PROTOCOL(V2): [%s] ignoring RDY after CLS in state ClientStateV2Closing",
			client)
		return nil, nil
	}

	if state != stateSubscribed {
		return nil, protocol.NewFatalClientErr(nil, "E_INVALID", "cannot RDY in current state")
	}

	// 初始为1, 携带参数里可以指定, 用来做流控
	count := int64(1)
	if len(params) > 1 {
		b10, err := protocol.ByteToBase10(params[1])
		if err != nil {
			return nil, protocol.NewFatalClientErr(err, "E_INVALID",
				fmt.Sprintf("RDY could not parse count %s", params[1]))
		}
		count = int64(b10)
	}

	if count < 0 || count > p.nsqd.getOpts().MaxRdyCount {
		// this needs to be a fatal error otherwise clients would have
		// inconsistent state
		return nil, protocol.NewFatalClientErr(nil, "E_INVALID",
			fmt.Sprintf("RDY count %d out of range 0-%d", count, p.nsqd.getOpts().MaxRdyCount))
	}

	client.SetReadyCount(count)

	return nil, nil
}

// 消息消费成功
func (p *protocolV2) FIN(client *clientV2, params [][]byte) ([]byte, error) {
	state := atomic.LoadInt32(&client.State)
	if state != stateSubscribed && state != stateClosing {
		return nil, protocol.NewFatalClientErr(nil, "E_INVALID", "cannot FIN in current state")
	}

	if len(params) < 2 {
		return nil, protocol.NewFatalClientErr(nil, "E_INVALID", "FIN insufficient number of params")
	}

	id, err := getMessageID(params[1])
	if err != nil {
		return nil, protocol.NewFatalClientErr(nil, "E_INVALID", err.Error())
	}

	err = client.Channel.FinishMessage(client.ID, *id)
	if err != nil {
		return nil, protocol.NewClientErr(err, "E_FIN_FAILED",
			fmt.Sprintf("FIN %s failed %s", *id, err.Error()))
	}

	// client状态更新
	client.FinishedMessage()

	return nil, nil
}

// 重新入队消息
func (p *protocolV2) REQ(client *clientV2, params [][]byte) ([]byte, error) {
	state := atomic.LoadInt32(&client.State)
	if state != stateSubscribed && state != stateClosing {
		return nil, protocol.NewFatalClientErr(nil, "E_INVALID", "cannot REQ in current state")
	}

	if len(params) < 3 {
		return nil, protocol.NewFatalClientErr(nil, "E_INVALID", "REQ insufficient number of params")
	}

	id, err := getMessageID(params[1])
	if err != nil {
		return nil, protocol.NewFatalClientErr(nil, "E_INVALID", err.Error())
	}

	timeoutMs, err := protocol.ByteToBase10(params[2])
	if err != nil {
		return nil, protocol.NewFatalClientErr(err, "E_INVALID",
			fmt.Sprintf("REQ could not parse timeout %s", params[2]))
	}
	timeoutDuration := time.Duration(timeoutMs) * time.Millisecond

	maxReqTimeout := p.nsqd.getOpts().MaxReqTimeout
	clampedTimeout := timeoutDuration

	if timeoutDuration < 0 {
		clampedTimeout = 0
	} else if timeoutDuration > maxReqTimeout {
		clampedTimeout = maxReqTimeout
	}
	if clampedTimeout != timeoutDuration {
		p.nsqd.logf(LOG_INFO, "PROTOCOL(V2): [%s] REQ timeout %d out of range 0-%d. Setting to %d",
			client, timeoutDuration, maxReqTimeout, clampedTimeout)
		timeoutDuration = clampedTimeout
	}

	err = client.Channel.RequeueMessage(client.ID, *id, timeoutDuration)
	if err != nil {
		return nil, protocol.NewClientErr(err, "E_REQ_FAILED",
			fmt.Sprintf("REQ %s failed %s", *id, err.Error()))
	}

	client.RequeuedMessage()

	return nil, nil
}

// 关闭客户端连接
func (p *protocolV2) CLS(client *clientV2, params [][]byte) ([]byte, error) {
	if atomic.LoadInt32(&client.State) != stateSubscribed {
		return nil, protocol.NewFatalClientErr(nil, "E_INVALID", "cannot CLS in current state")
	}

	client.StartClose()

	return []byte("CLOSE_WAIT"), nil
}

// 什么也不做
func (p *protocolV2) NOP(client *clientV2, params [][]byte) ([]byte, error) {
	return nil, nil
}

// 投递消息到topic
func (p *protocolV2) PUB(client *clientV2, params [][]byte) ([]byte, error) {
	var err error

	if len(params) < 2 {
		return nil, protocol.NewFatalClientErr(nil, "E_INVALID", "PUB insufficient number of parameters")
	}

	topicName := string(params[1])
	if !protocol.IsValidTopicName(topicName) {
		return nil, protocol.NewFatalClientErr(nil, "E_BAD_TOPIC",
			fmt.Sprintf("PUB topic name %q is not valid", topicName))
	}

	bodyLen, err := readLen(client.Reader, client.lenSlice)
	if err != nil {
		return nil, protocol.NewFatalClientErr(err, "E_BAD_MESSAGE", "PUB failed to read message body size")
	}

	if bodyLen <= 0 {
		return nil, protocol.NewFatalClientErr(nil, "E_BAD_MESSAGE",
			fmt.Sprintf("PUB invalid message body size %d", bodyLen))
	}

	if int64(bodyLen) > p.nsqd.getOpts().MaxMsgSize {
		return nil, protocol.NewFatalClientErr(nil, "E_BAD_MESSAGE",
			fmt.Sprintf("PUB message too big %d > %d", bodyLen, p.nsqd.getOpts().MaxMsgSize))
	}

	messageBody := make([]byte, bodyLen)
	_, err = io.ReadFull(client.Reader, messageBody)
	if err != nil {
		return nil, protocol.NewFatalClientErr(err, "E_BAD_MESSAGE", "PUB failed to read message body")
	}

	if err := p.CheckAuth(client, "PUB", topicName, ""); err != nil {
		return nil, err
	}

	topic := p.nsqd.GetTopic(topicName)
	msg := NewMessage(topic.GenerateID(), messageBody)
	err = topic.PutMessage(msg)
	if err != nil {
		return nil, protocol.NewFatalClientErr(err, "E_PUB_FAILED", "PUB failed "+err.Error())
	}

	client.PublishedMessage(topicName, 1)

	return okBytes, nil
}

func (p *protocolV2) MPUB(client *clientV2, params [][]byte) ([]byte, error) {
	var err error

	if len(params) < 2 {
		return nil, protocol.NewFatalClientErr(nil, "E_INVALID", "MPUB insufficient number of parameters")
	}

	topicName := string(params[1])
	if !protocol.IsValidTopicName(topicName) {
		return nil, protocol.NewFatalClientErr(nil, "E_BAD_TOPIC",
			fmt.Sprintf("E_BAD_TOPIC MPUB topic name %q is not valid", topicName))
	}

	if err := p.CheckAuth(client, "MPUB", topicName, ""); err != nil {
		return nil, err
	}

	topic := p.nsqd.GetTopic(topicName)

	bodyLen, err := readLen(client.Reader, client.lenSlice)
	if err != nil {
		return nil, protocol.NewFatalClientErr(err, "E_BAD_BODY", "MPUB failed to read body size")
	}

	if bodyLen <= 0 {
		return nil, protocol.NewFatalClientErr(nil, "E_BAD_BODY",
			fmt.Sprintf("MPUB invalid body size %d", bodyLen))
	}

	if int64(bodyLen) > p.nsqd.getOpts().MaxBodySize {
		return nil, protocol.NewFatalClientErr(nil, "E_BAD_BODY",
			fmt.Sprintf("MPUB body too big %d > %d", bodyLen, p.nsqd.getOpts().MaxBodySize))
	}

	messages, err := readMPUB(client.Reader, client.lenSlice, topic,
		p.nsqd.getOpts().MaxMsgSize, p.nsqd.getOpts().MaxBodySize)
	if err != nil {
		return nil, err
	}

	// if we've made it this far we've validated all the input,
	// the only possible error is that the topic is exiting during
	// this next call (and no messages will be queued in that case)
	err = topic.PutMessages(messages)
	if err != nil {
		return nil, protocol.NewFatalClientErr(err, "E_MPUB_FAILED", "MPUB failed "+err.Error())
	}

	client.PublishedMessage(topicName, uint64(len(messages)))

	return okBytes, nil
}

func (p *protocolV2) DPUB(client *clientV2, params [][]byte) ([]byte, error) {
	var err error

	if len(params) < 3 {
		return nil, protocol.NewFatalClientErr(nil, "E_INVALID", "DPUB insufficient number of parameters")
	}

	topicName := string(params[1])
	if !protocol.IsValidTopicName(topicName) {
		return nil, protocol.NewFatalClientErr(nil, "E_BAD_TOPIC",
			fmt.Sprintf("DPUB topic name %q is not valid", topicName))
	}

	timeoutMs, err := protocol.ByteToBase10(params[2])
	if err != nil {
		return nil, protocol.NewFatalClientErr(err, "E_INVALID",
			fmt.Sprintf("DPUB could not parse timeout %s", params[2]))
	}
	timeoutDuration := time.Duration(timeoutMs) * time.Millisecond

	if timeoutDuration < 0 || timeoutDuration > p.nsqd.getOpts().MaxReqTimeout {
		return nil, protocol.NewFatalClientErr(nil, "E_INVALID",
			fmt.Sprintf("DPUB timeout %d out of range 0-%d",
				timeoutMs, p.nsqd.getOpts().MaxReqTimeout/time.Millisecond))
	}

	bodyLen, err := readLen(client.Reader, client.lenSlice)
	if err != nil {
		return nil, protocol.NewFatalClientErr(err, "E_BAD_MESSAGE", "DPUB failed to read message body size")
	}

	if bodyLen <= 0 {
		return nil, protocol.NewFatalClientErr(nil, "E_BAD_MESSAGE",
			fmt.Sprintf("DPUB invalid message body size %d", bodyLen))
	}

	if int64(bodyLen) > p.nsqd.getOpts().MaxMsgSize {
		return nil, protocol.NewFatalClientErr(nil, "E_BAD_MESSAGE",
			fmt.Sprintf("DPUB message too big %d > %d", bodyLen, p.nsqd.getOpts().MaxMsgSize))
	}

	messageBody := make([]byte, bodyLen)
	_, err = io.ReadFull(client.Reader, messageBody)
	if err != nil {
		return nil, protocol.NewFatalClientErr(err, "E_BAD_MESSAGE", "DPUB failed to read message body")
	}

	if err := p.CheckAuth(client, "DPUB", topicName, ""); err != nil {
		return nil, err
	}

	topic := p.nsqd.GetTopic(topicName)
	msg := NewMessage(topic.GenerateID(), messageBody)
	msg.deferred = timeoutDuration
	err = topic.PutMessage(msg)
	if err != nil {
		return nil, protocol.NewFatalClientErr(err, "E_DPUB_FAILED", "DPUB failed "+err.Error())
	}

	client.PublishedMessage(topicName, 1)

	return okBytes, nil
}

// 重新设置一条消息的过期时间
func (p *protocolV2) TOUCH(client *clientV2, params [][]byte) ([]byte, error) {
	state := atomic.LoadInt32(&client.State)
	if state != stateSubscribed && state != stateClosing {
		return nil, protocol.NewFatalClientErr(nil, "E_INVALID", "cannot TOUCH in current state")
	}

	if len(params) < 2 {
		return nil, protocol.NewFatalClientErr(nil, "E_INVALID", "TOUCH insufficient number of params")
	}

	// 获取消息id
	id, err := getMessageID(params[1])
	if err != nil {
		return nil, protocol.NewFatalClientErr(nil, "E_INVALID", err.Error())
	}

	client.writeLock.RLock()
	msgTimeout := client.MsgTimeout
	client.writeLock.RUnlock()
	err = client.Channel.TouchMessage(client.ID, *id, msgTimeout)
	if err != nil {
		return nil, protocol.NewClientErr(err, "E_TOUCH_FAILED",
			fmt.Sprintf("TOUCH %s failed %s", *id, err.Error()))
	}

	return nil, nil
}

// 被MPUB调用，从tcp数据中解出消息数组
func readMPUB(r io.Reader, tmp []byte, topic *Topic, maxMessageSize int64, maxBodySize int64) ([]*Message, error) {
	numMessages, err := readLen(r, tmp)
	if err != nil {
		return nil, protocol.NewFatalClientErr(err, "E_BAD_BODY", "MPUB failed to read message count")
	}

	// 4 == total num, 5 == length + min 1
	maxMessages := (maxBodySize - 4) / 5
	if numMessages <= 0 || int64(numMessages) > maxMessages {
		return nil, protocol.NewFatalClientErr(err, "E_BAD_BODY",
			fmt.Sprintf("MPUB invalid message count %d", numMessages))
	}

	messages := make([]*Message, 0, numMessages)
	for i := int32(0); i < numMessages; i++ {
		messageSize, err := readLen(r, tmp)
		if err != nil {
			return nil, protocol.NewFatalClientErr(err, "E_BAD_MESSAGE",
				fmt.Sprintf("MPUB failed to read message(%d) body size", i))
		}

		if messageSize <= 0 {
			return nil, protocol.NewFatalClientErr(nil, "E_BAD_MESSAGE",
				fmt.Sprintf("MPUB invalid message(%d) body size %d", i, messageSize))
		}

		if int64(messageSize) > maxMessageSize {
			return nil, protocol.NewFatalClientErr(nil, "E_BAD_MESSAGE",
				fmt.Sprintf("MPUB message too big %d > %d", messageSize, maxMessageSize))
		}

		msgBody := make([]byte, messageSize)
		_, err = io.ReadFull(r, msgBody)
		if err != nil {
			return nil, protocol.NewFatalClientErr(err, "E_BAD_MESSAGE", "MPUB failed to read message body")
		}

		messages = append(messages, NewMessage(topic.GenerateID(), msgBody))
	}

	return messages, nil
}

// validate and cast the bytes on the wire to a message ID
func getMessageID(p []byte) (*MessageID, error) {
	if len(p) != MsgIDLength {
		return nil, errors.New("invalid message ID")
	}
	return (*MessageID)(unsafe.Pointer(&p[0])), nil
}

func readLen(r io.Reader, tmp []byte) (int32, error) {
	_, err := io.ReadFull(r, tmp)
	if err != nil {
		return 0, err
	}
	return int32(binary.BigEndian.Uint32(tmp)), nil
}

func enforceTLSPolicy(client *clientV2, p *protocolV2, command []byte) error {
	if p.nsqd.getOpts().TLSRequired != TLSNotRequired && atomic.LoadInt32(&client.TLS) != 1 {
		return protocol.NewFatalClientErr(nil, "E_INVALID",
			fmt.Sprintf("cannot %s in current state (TLS required)", command))
	}
	return nil
}
