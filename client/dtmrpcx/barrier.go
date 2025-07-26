package dtmrpcx

import (
	"github.com/dtm-labs/dtm/client/dtmcli"
	"github.com/dtm-labs/dtm/client/dtmrpcx/dtmrimp"
	"golang.org/x/net/context"
)

// BarrierFromRpcX generate a Barrier from grpc context
func BarrierFromRpcX(ctx context.Context) (*dtmcli.BranchBarrier, error) {
	t := dtmrimp.TransBaseFromRpcX(ctx)
	return dtmcli.BarrierFrom(t.TransType, t.Gid, t.BranchID, t.Op)
}
