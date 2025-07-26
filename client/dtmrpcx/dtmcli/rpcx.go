package dtmcli

import (
	"github.com/dtm-labs/dtm/client/dtmgrpc/dtmgpb"
	rpcXClient "github.com/smallnest/rpcx/client"
	"golang.org/x/net/context"
	"google.golang.org/grpc"
	"google.golang.org/protobuf/types/known/emptypb"
)

type Client struct {
	xc rpcXClient.XClient
}

func NewRpcXClient(client rpcXClient.XClient) Client {
	return Client{
		xc: client,
	}
}

func (c *Client) NewGid(ctx context.Context, in *emptypb.Empty, opts ...grpc.CallOption) (*dtmgpb.DtmGidReply, error) {
	reply := &dtmgpb.DtmGidReply{}
	if err := c.xc.Call(ctx, "NewGid", in, reply); err != nil {
		return nil, err
	}
	return reply, nil
}

func (c *Client) Prepare(ctx context.Context, in *dtmgpb.DtmRequest, opts ...grpc.CallOption) (*emptypb.Empty, error) {
	reply := &emptypb.Empty{}
	if err := c.xc.Call(ctx, "Prepare", in, reply); err != nil {
		return nil, err
	}
	return reply, nil
}

func (c *Client) Abort(ctx context.Context, in *dtmgpb.DtmRequest, opts ...grpc.CallOption) (*emptypb.Empty, error) {
	reply := &emptypb.Empty{}
	if err := c.xc.Call(ctx, "Abort", in, reply); err != nil {
		return nil, err
	}
	return reply, nil
}

func (c *Client) RegisterBranch(ctx context.Context, in *dtmgpb.DtmBranchRequest, opts ...grpc.CallOption) (*emptypb.Empty, error) {
	reply := &emptypb.Empty{}
	if err := c.xc.Call(ctx, "RegisterBranch", in, reply); err != nil {
		return nil, err
	}
	return reply, nil
}

func (c *Client) PrepareWorkflow(ctx context.Context, in *dtmgpb.DtmRequest, opts ...grpc.CallOption) (*dtmgpb.DtmProgressesReply, error) {
	reply := &dtmgpb.DtmProgressesReply{}
	if err := c.xc.Call(ctx, "PrepareWorkflow", in, reply); err != nil {
		return nil, err
	}
	return reply, nil
}

func (c *Client) Subscribe(ctx context.Context, in *dtmgpb.DtmTopicRequest, opts ...grpc.CallOption) (*emptypb.Empty, error) {
	reply := &emptypb.Empty{}
	if err := c.xc.Call(ctx, "Subscribe", in, reply); err != nil {
		return nil, err
	}
	return reply, nil
}

func (c *Client) Unsubscribe(ctx context.Context, in *dtmgpb.DtmTopicRequest, opts ...grpc.CallOption) (*emptypb.Empty, error) {
	reply := &emptypb.Empty{}
	if err := c.xc.Call(ctx, "Unsubscribe", in, reply); err != nil {
		return nil, err
	}
	return reply, nil
}

func (c *Client) DeleteTopic(ctx context.Context, in *dtmgpb.DtmTopicRequest, opts ...grpc.CallOption) (*emptypb.Empty, error) {
	reply := &emptypb.Empty{}
	if err := c.xc.Call(ctx, "NewGid", in, reply); err != nil {
		return nil, err
	}
	return reply, nil
}
