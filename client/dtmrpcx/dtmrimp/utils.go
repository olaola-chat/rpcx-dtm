package dtmrimp

import (
	"context"
	"fmt"
	"github.com/dtm-labs/dtm/client/dtmcli/dtmimp"
	"github.com/dtm-labs/dtm/client/dtmgrpc/dtmgpb"
	"github.com/dtm-labs/dtmdriver"
	"github.com/dtm-labs/logger"
	"google.golang.org/grpc/metadata"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/emptypb"
	"net"
)

// MustProtoMarshal must version of proto.Marshal
func MustProtoMarshal(msg proto.Message) []byte {
	b, err := proto.Marshal(msg)
	dtmimp.PanicIf(err != nil, err)
	return b
}

// MustProtoUnmarshal must version of proto.Unmarshal
func MustProtoUnmarshal(data []byte, msg proto.Message) {
	err := proto.Unmarshal(data, msg)
	dtmimp.PanicIf(err != nil, err)
}

// GetDtmRequest return a DtmRequest from TransBase
func GetDtmRequest(s *dtmimp.TransBase) *dtmgpb.DtmRequest {
	return &dtmgpb.DtmRequest{
		Gid:       s.Gid,
		TransType: s.TransType,
		TransOptions: &dtmgpb.DtmTransOptions{
			WaitResult:     s.WaitResult,
			TimeoutToFail:  s.TimeoutToFail,
			RetryInterval:  s.RetryInterval,
			BranchHeaders:  s.BranchHeaders,
			RequestTimeout: s.RequestTimeout,
			RetryLimit:     s.RetryLimit,
		},
		QueryPrepared:  s.QueryPrepared,
		CustomedData:   s.CustomData,
		BinPayloads:    s.BinPayloads,
		Steps:          dtmimp.MustMarshalString(s.Steps),
		RollbackReason: s.RollbackReason,
	}
}

// DtmRpcXCall make a convenient call to dtm
func DtmRpcXCall(s *dtmimp.TransBase, operation string) error {
	reply := emptypb.Empty{}
	return MustGetRpcXClient(s.Dtm).Call(s.Context, operation, GetDtmRequest(s), &reply)
}

const dtmpre string = "dtm-"

// TransInfo2Ctx add trans info to grpc context
func TransInfo2Ctx(ctx context.Context, gid, transType, branchID, op, dtm string) context.Context {
	nctx := ctx
	if ctx == nil {
		nctx = context.Background()
	}
	return metadata.AppendToOutgoingContext(
		nctx,
		dtmpre+"gid", gid,
		dtmpre+"trans_type", transType,
		dtmpre+"branch_id", branchID,
		dtmpre+"op", op,
		dtmpre+"dtm", dtm,
	)
}

// Map2Kvs map to metadata kv
func Map2Kvs(m map[string]string) []string {
	kvs := make([]string, 0, len(m)*2)
	for k, v := range m {
		kvs = append(kvs, k, v)
	}
	return kvs
}

// LogDtmCtx logout dtm info in context metadata
func LogDtmCtx(ctx context.Context) {
	tb := TransBaseFromRpcX(ctx)
	if tb.Gid != "" {
		logger.Debugf("gid: %s trans_type: %s branch_id: %s op: %s dtm: %s", tb.Gid, tb.TransType, tb.BranchID, tb.Op, tb.Dtm)
	}
}

func dtmGet(md metadata.MD, key string) string {
	return mdGet(md, dtmpre+key)
}

func mdGet(md metadata.MD, key string) string {
	v := md.Get(key)
	if len(v) == 0 {
		return ""
	}
	return v[0]
}

// TransBaseFromRpcX get trans base info from a context metadata
func TransBaseFromRpcX(ctx context.Context) *dtmimp.TransBase {
	md, _ := metadata.FromIncomingContext(ctx)
	tb := dtmimp.NewTransBase(dtmGet(md, "gid"), dtmGet(md, "trans_type"), dtmGet(md, "dtm"), dtmGet(md, "branch_id"))
	tb.Op = dtmGet(md, "op")
	return tb
}

// GetMetaFromContext get header from context
func GetMetaFromContext(ctx context.Context, name string) string {
	md, _ := metadata.FromIncomingContext(ctx)
	return mdGet(md, name)
}

// GetDtmMetaFromContext get dtm header from context
func GetDtmMetaFromContext(ctx context.Context, name string) string {
	md, _ := metadata.FromIncomingContext(ctx)
	return dtmGet(md, name)
}

type requestTimeoutKey struct{}

// RequestTimeoutFromContext returns requestTime of transOption option
func RequestTimeoutFromContext(ctx context.Context) int64 {
	if v, ok := ctx.Value(requestTimeoutKey{}).(int64); ok {
		return v
	}

	return 0
}

// RequestTimeoutNewContext sets requestTimeout of transOption option to context
func RequestTimeoutNewContext(ctx context.Context, requestTimeout int64) context.Context {
	return context.WithValue(ctx, requestTimeoutKey{}, requestTimeout)
}

func InvokeBranch(t *dtmimp.TransBase, isRaw bool, msg proto.Message, url string, reply interface{}, branchID string, op string) error {
	server, method, err := dtmdriver.GetDriver().ParseServerMethod(url)
	if err != nil {
		return err
	}
	ctx := TransInfo2Ctx(t.Context, t.Gid, t.TransType, branchID, op, t.Dtm)
	ctx = metadata.AppendToOutgoingContext(ctx, Map2Kvs(t.BranchHeaders)...)
	if t.TransType == "xa" { // xa branch need additional phase2_url
		ctx = metadata.AppendToOutgoingContext(ctx, Map2Kvs(map[string]string{dtmpre + "phase2_url": url})...)
	}
	return MustGetRpcXClient(server).Call(ctx, method, msg, reply)
}

func LocalIPv4s() (string, error) {
	addrs, err := net.InterfaceAddrs()
	if err != nil {
		return "", err
	}

	for _, a := range addrs {
		if ipnet, ok := a.(*net.IPNet); ok && !ipnet.IP.IsLoopback() && ipnet.IP.To4() != nil {
			return ipnet.IP.String(), nil
		}
	}

	return "", fmt.Errorf("empty found in InterfaceAddrs")
}
