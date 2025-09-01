package dtmsvr

import (
	"fmt"
	"github.com/dtm-labs/dtm/client/dtmcli"
	"github.com/dtm-labs/dtm/client/dtmcli/dtmimp"
	pb "github.com/dtm-labs/dtm/client/dtmgrpc/dtmgpb"
	"github.com/dtm-labs/dtm/client/dtmrpcx"
	"github.com/dtm-labs/dtm/client/dtmrpcx/dtmrimp"
	"github.com/dtm-labs/logger"
	"github.com/rcrowley/go-metrics"
	rpcXServer "github.com/smallnest/rpcx/server"
	"github.com/smallnest/rpcx/serverplugin"
	"golang.org/x/net/context"
	"google.golang.org/protobuf/types/known/emptypb"
	"time"
)

// DtmRpcXServer is used to implement dtmgimp.DtmServer.
type DtmRpcXServer struct {
	impl   *rpcXServer.Server
	server string
	addr   string
}

func NewRpcXServer(server string) *DtmRpcXServer {
	return &DtmRpcXServer{
		impl: rpcXServer.NewServer(
			rpcXServer.WithReadTimeout(time.Second*3),
			rpcXServer.WithWriteTimeout(time.Second*3)),
	}
}

func (s *DtmRpcXServer) Serve(addr string) {
	s.impl.Plugins.Add(dtmrimp.NewThrottling(10000, 20000))
	s.impl.Plugins.Add(dtmrimp.OpenTracingPlugin{})
	cc := dtmrimp.GetConsulConfig()

	logger.Infof("starting dtm rpcx server at %s, prefix = %s", cc.Address, cc.Prefix)
	discover := &serverplugin.ConsulRegisterPlugin{
		ServiceAddress: fmt.Sprintf("tcp@%s", s.addr),
		ConsulServers:  []string{cc.Address},
		BasePath:       cc.Prefix,
		Metrics:        metrics.DefaultRegistry,
		UpdateInterval: time.Second * 10, //这个更新的是Metrics
	}
	dtmimp.E2P(discover.Start())
	s.impl.Plugins.Add(discover)
	s.impl.RegisterOnShutdown(func(s *rpcXServer.Server) {
		for _, v := range s.Plugins.All() {
			if consulPlugin, ok := v.(*serverplugin.ConsulRegisterPlugin); ok {
				if stopErr := consulPlugin.Stop(); stopErr != nil {
					logger.Errorf("server consulPlugin stop failed, err:%v", stopErr)
				}
			}
		}
	})
	dtmimp.E2P(s.impl.RegisterName("Dtm", NewRpcXServer(s.server), fmt.Sprintf("group=%s", conf.RunMode)))
	dtmimp.E2P(s.impl.Serve("tcp", addr))
}

func (s *DtmRpcXServer) NewGid(ctx context.Context, in *emptypb.Empty, reply *pb.DtmGidReply) error {
	reply.Gid = GenGid()
	return nil
}

func (s *DtmRpcXServer) Submit(ctx context.Context, in *pb.DtmRequest, reply *emptypb.Empty) error {
	r := svcSubmit(TransFromDtmRequest(ctx, in))
	return dtmrpcx.FromDtmError(r)
}

func (s *DtmRpcXServer) Prepare(ctx context.Context, in *pb.DtmRequest, reply *emptypb.Empty) error {
	r := svcPrepare(TransFromDtmRequest(ctx, in))
	return dtmrpcx.FromDtmError(r)
}

func (s *DtmRpcXServer) Abort(ctx context.Context, in *pb.DtmRequest, reply *emptypb.Empty) error {
	r := svcAbort(TransFromDtmRequest(ctx, in))
	return dtmrpcx.FromDtmError(r)
}

func (s *DtmRpcXServer) RegisterBranch(ctx context.Context, in *pb.DtmBranchRequest, reply *emptypb.Empty) error {
	r := svcRegisterBranch(in.TransType, &TransBranch{
		Gid:      in.Gid,
		BranchID: in.BranchID,
		Status:   dtmcli.StatusPrepared,
		BinData:  in.BusiPayload,
	}, in.Data)
	return dtmrpcx.FromDtmError(r)
}

func (s *DtmRpcXServer) PrepareWorkflow(ctx context.Context, in *pb.DtmRequest, reply *pb.DtmProgressesReply) error {
	trans, branches, err := svcPrepareWorkflow(TransFromDtmRequest(ctx, in))
	reply.Transaction = &pb.DtmTransaction{
		Gid:            trans.Gid,
		Status:         trans.Status,
		RollbackReason: trans.RollbackReason,
	}
	reply.Progresses = []*pb.DtmProgress{}
	for _, b := range branches {
		reply.Progresses = append(reply.Progresses, &pb.DtmProgress{
			Status:   b.Status,
			BranchID: b.BranchID,
			Op:       b.Op,
			BinData:  b.BinData,
		})
	}
	return dtmrpcx.FromDtmError(err)
}

func (s *DtmRpcXServer) Subscribe(ctx context.Context, in *pb.DtmTopicRequest, reply *emptypb.Empty) error {
	return dtmrpcx.FromDtmError(Subscribe(in.Topic, in.URL, in.Remark))
}

func (s *DtmRpcXServer) Unsubscribe(ctx context.Context, in *pb.DtmTopicRequest, reply *emptypb.Empty) error {
	return dtmrpcx.FromDtmError(Unsubscribe(in.Topic, in.URL))
}

func (s *DtmRpcXServer) DeleteTopic(ctx context.Context, in *pb.DtmTopicRequest, reply *emptypb.Empty) error {
	return dtmrpcx.FromDtmError(GetStore().DeleteKV(topicsCat, in.Topic))
}
