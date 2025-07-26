package dtmrpcx

import (
	"database/sql"
	"fmt"
	"github.com/dtm-labs/dtm/client/dtmcli"
	"github.com/dtm-labs/dtm/client/dtmcli/dtmimp"
	"github.com/dtm-labs/dtm/client/dtmgrpc/dtmgimp"
	"github.com/dtm-labs/dtm/client/dtmgrpc/dtmgpb"
	"golang.org/x/net/context"
	"google.golang.org/grpc"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/emptypb"
)

// XaRpcXGlobalFunc type of xa global function
type XaRpcXGlobalFunc func(xa *XaRpcX) error

// XaRpcXLocalFunc type of xa local function
type XaRpcXLocalFunc func(db *sql.DB, xa *XaRpcX) error

// XaRpcX xa transaction
type XaRpcX struct {
	dtmimp.TransBase
	Phase2URL string
}

// XaRpcXFromRequest construct xa info from request
func XaRpcXFromRequest(ctx context.Context) (*XaRpcX, error) {
	xa := &XaRpcX{
		TransBase: *dtmgimp.TransBaseFromRpcX(ctx),
	}
	xa.Phase2URL = dtmgimp.GetDtmMetaFromContext(ctx, "phase2_url")
	if xa.Gid == "" || xa.BranchID == "" || xa.Op == "" {
		return nil, fmt.Errorf("bad xa info: gid: %s branchid: %s op: %s phase2_url: %s", xa.Gid, xa.BranchID, xa.Op, xa.Phase2URL)
	}
	return xa, nil
}

// XaLocalTransaction start a xa local transaction
func XaLocalTransaction(ctx context.Context, dbConf dtmcli.DBConf, xaFunc XaRpcXLocalFunc) error {
	xa, err := XaRpcXFromRequest(ctx)
	if err != nil {
		return err
	}
	if xa.Op == dtmimp.OpCommit || xa.Op == dtmimp.OpRollback {
		return dtmimp.XaHandlePhase2(xa.Gid, dbConf, xa.BranchID, xa.Op)
	}
	return dtmimp.XaHandleLocalTrans(&xa.TransBase, dbConf, func(db *sql.DB) error {
		err := xaFunc(db, xa)
		if err != nil {
			return err
		}
		_, err = dtmgimp.MustGetDtmClient(xa.Dtm).RegisterBranch(context.Background(), &dtmgpb.DtmBranchRequest{
			Gid:         xa.Gid,
			BranchID:    xa.BranchID,
			TransType:   xa.TransType,
			BusiPayload: nil,
			Data:        map[string]string{"url": xa.Phase2URL},
		})
		return err
	})
}

// XaGlobalTransaction start a xa global transaction
func XaGlobalTransaction(server string, gid string, xaFunc XaRpcXGlobalFunc) error {
	return XaGlobalTransaction2(server, gid, func(xg *XaRpcX) {}, xaFunc)
}

// XaGlobalTransaction2 new version of XaGlobalTransaction. support custom
func XaGlobalTransaction2(server string, gid string, custom func(*XaRpcX), xaFunc XaRpcXGlobalFunc) error {
	xa := &XaRpcX{TransBase: *dtmimp.NewTransBase(gid, "xa", server, "")}
	custom(xa)
	dc := dtmgimp.MustGetDtmClient(xa.Dtm)
	req := dtmgimp.GetDtmRequest(&xa.TransBase)
	return dtmimp.XaHandleGlobalTrans(&xa.TransBase, func(action string) error {
		f := map[string]func(context.Context, *dtmgpb.DtmRequest, ...grpc.CallOption) (*emptypb.Empty, error){
			"prepare": dc.Prepare,
			"submit":  dc.Submit,
			"abort":   dc.Abort,
		}[action]
		_, err := f(context.Background(), req)
		return err
	}, func() error {
		return xaFunc(xa)
	})
}

// CallBranch call a xa branch
func (x *XaRpcX) CallBranch(msg proto.Message, url string, reply interface{}, opts ...grpc.CallOption) error {
	return dtmgimp.InvokeBranch(&x.TransBase, false, msg, url, reply, x.NewSubBranchID(), "action", opts...)
}
