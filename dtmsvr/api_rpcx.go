package dtmsvr

import (
	"github.com/dtm-labs/dtm/client/dtmcli"
	pb "github.com/dtm-labs/dtm/client/dtmgrpc/dtmgpb"
	"github.com/dtm-labs/dtm/client/dtmrpcx"
	"golang.org/x/net/context"
	"google.golang.org/protobuf/types/known/emptypb"
)

// dtmRpcXServer is used to implement dtmgimp.DtmServer.
type dtmRpcXServer struct {
	pb.UnimplementedDtmServer
}

func (s *dtmRpcXServer) NewGid(ctx context.Context, in *emptypb.Empty, reply *pb.DtmGidReply) error {
	reply.Gid = GenGid()
	return nil
}

func (s *dtmRpcXServer) Submit(ctx context.Context, in *pb.DtmRequest, reply *emptypb.Empty) error {
	r := svcSubmit(TransFromDtmRequest(ctx, in))
	return dtmrpcx.FromDtmError(r)
}

func (s *dtmRpcXServer) Prepare(ctx context.Context, in *pb.DtmRequest, reply *emptypb.Empty) error {
	r := svcPrepare(TransFromDtmRequest(ctx, in))
	return dtmrpcx.FromDtmError(r)
}

func (s *dtmRpcXServer) Abort(ctx context.Context, in *pb.DtmRequest, reply *emptypb.Empty) error {
	r := svcAbort(TransFromDtmRequest(ctx, in))
	return dtmrpcx.FromDtmError(r)
}

func (s *dtmRpcXServer) RegisterBranch(ctx context.Context, in *pb.DtmBranchRequest, reply *emptypb.Empty) error {
	r := svcRegisterBranch(in.TransType, &TransBranch{
		Gid:      in.Gid,
		BranchID: in.BranchID,
		Status:   dtmcli.StatusPrepared,
		BinData:  in.BusiPayload,
	}, in.Data)
	return dtmrpcx.FromDtmError(r)
}

func (s *dtmRpcXServer) PrepareWorkflow(ctx context.Context, in *pb.DtmRequest, reply *pb.DtmProgressesReply) error {
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

func (s *dtmRpcXServer) Subscribe(ctx context.Context, in *pb.DtmTopicRequest, reply *emptypb.Empty) error {
	return dtmrpcx.FromDtmError(Subscribe(in.Topic, in.URL, in.Remark))
}

func (s *dtmRpcXServer) Unsubscribe(ctx context.Context, in *pb.DtmTopicRequest, reply *emptypb.Empty) error {
	return dtmrpcx.FromDtmError(Unsubscribe(in.Topic, in.URL))
}

func (s *dtmRpcXServer) DeleteTopic(ctx context.Context, in *pb.DtmTopicRequest, reply *emptypb.Empty) error {
	return dtmrpcx.FromDtmError(GetStore().DeleteKV(topicsCat, in.Topic))
}
