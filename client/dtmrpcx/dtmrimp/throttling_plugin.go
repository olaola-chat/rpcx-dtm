package dtmrimp

import (
	"context"
	"fmt"
	"github.com/juju/ratelimit"
	"github.com/smallnest/rpcx/protocol"
	"net"
)

// Throttling 定义了有个RPC server插件
type Throttling struct {
	bucket *ratelimit.Bucket
}

func NewThrottling(limit, capacity int64) *Throttling {
	return &Throttling{
		bucket: ratelimit.NewBucketWithRate(float64(limit), capacity),
	}
}

// NewInfoPlugin 实例化一个 InfoHandler

// HeartbeatRequest 心跳的回调
func (h *Throttling) HeartbeatRequest(ctx context.Context, req *protocol.Message) error {
	//conn := ctx.Value(server.RemoteConnContextKey).(net.Conn)
	//println("OnHeartbeat:", conn.RemoteAddr().String(), req.SerializeType())
	return nil
}

// HandleConnAccept 当有客户端建立链接时回调
func (h *Throttling) HandleConnAccept(conn net.Conn) (net.Conn, bool) {
	return conn, true
}

// HandleConnClose 当有客户端关闭链接时回调
func (h *Throttling) HandleConnClose(conn net.Conn) bool {
	return true
}

// PreCall 当有客户端发起函数调用是回调
// 这里是限流
func (h *Throttling) PreCall(ctx context.Context, serviceName, methodName string, args interface{}) (interface{}, error) {
	//conn, ok := ctx.Value(server.RemoteConnContextKey).(net.Conn)
	//if ok {
	//	g.Log().Println("call", conn.RemoteAddr().String(), serviceName, methodName)
	//}
	ok := h.bucket.TakeAvailable(1) > 0
	var err error = nil
	if !ok {
		err = fmt.Errorf("rpc call is limited, service %s %s", serviceName, methodName)
	}
	return args, err
}
