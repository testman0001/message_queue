package nsqlookupd

import (
	"io"
	"net"
	"time"

	"github.com/youzan/nsq/internal/protocol"
)

type tcpServer struct {
	ctx *Context
}

func (p *tcpServer) Handle(clientConn net.Conn) {
	nsqlookupLog.Logf("TCP: new client(%s)", clientConn.RemoteAddr())

	// The client should initialize itself by sending a 4 byte sequence indicating
	// the version of the protocol that it intends to communicate, this will allow us
	// to gracefully upgrade the protocol away from text/line oriented to whatever...
	buf := make([]byte, 4)
	clientConn.SetReadDeadline(time.Now().Add(time.Second * 3))
	_, err := io.ReadFull(clientConn, buf)
	if err != nil {
		nsqlookupLog.Logf(" failed to read protocol version - %s from client: %v", err, clientConn.RemoteAddr())
		clientConn.Close()
		return
	}
	protocolMagic := string(buf)

	nsqlookupLog.Logf("CLIENT(%s): desired protocol magic '%s'",
		clientConn.RemoteAddr(), protocolMagic)

	var prot protocol.Protocol
	switch protocolMagic {
	case "  V1":
		prot = &LookupProtocolV1{ctx: p.ctx}
	default:
		protocol.SendResponse(clientConn, []byte("E_BAD_PROTOCOL"))
		clientConn.Close()
		nsqlookupLog.LogErrorf(" client(%s) bad protocol magic '%s'",
			clientConn.RemoteAddr(), protocolMagic)
		return
	}

	err = prot.IOLoop(clientConn)
	if err != nil {
		if err == io.EOF {
			nsqlookupLog.Logf(" client(%s) - %s", clientConn.RemoteAddr(), err)
		} else {
			nsqlookupLog.LogWarningf(" client(%s) - %s", clientConn.RemoteAddr(), err)
		}
		return
	}
}
