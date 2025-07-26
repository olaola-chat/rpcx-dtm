package dtmrpcx

import (
	"github.com/dtm-labs/dtm/client/dtmcli"
	"github.com/dtm-labs/dtm/client/dtmgrpc/dtmgimp"
	"golang.org/x/net/context"
	"google.golang.org/protobuf/proto"
)

// SagaRpcX struct of saga
type SagaRpcX struct {
	dtmcli.Saga
}

// NewSagaRpcX create a saga
func NewSagaRpcX(server string, gid string, opts ...TransBaseOption) *SagaRpcX {
	sg := &SagaRpcX{Saga: *dtmcli.NewSaga(server, gid)}

	for _, opt := range opts {
		opt(&sg.TransBase)
	}

	return sg
}

// NewSagaRpcXWithContext create a saga with context
func NewSagaRpcXWithContext(ctx context.Context, server string, gid string, opts ...TransBaseOption) *SagaRpcX {
	sg := &SagaRpcX{Saga: *dtmcli.NewSagaWithContext(ctx, server, gid)}

	for _, opt := range opts {
		opt(&sg.TransBase)
	}

	return sg
}

// Add add a saga step
func (s *SagaRpcX) Add(action string, compensate string, payload proto.Message) *SagaRpcX {
	s.Steps = append(s.Steps, map[string]string{"action": action, "compensate": compensate})
	s.BinPayloads = append(s.BinPayloads, dtmgimp.MustProtoMarshal(payload))
	return s
}

// AddBranchOrder specify that branch should be after preBranches. branch should is larger than all the element in preBranches
func (s *SagaRpcX) AddBranchOrder(branch int, preBranches []int) *SagaRpcX {
	s.Saga.AddBranchOrder(branch, preBranches)
	return s
}

// EnableConcurrent enable the concurrent exec of sub trans
func (s *SagaRpcX) EnableConcurrent() *SagaRpcX {
	s.Saga.SetConcurrent()
	return s
}

// Submit submit the saga trans
func (s *SagaRpcX) Submit() error {
	s.Saga.BuildCustomOptions()
	return dtmgimp.DtmGrpcCall(&s.Saga.TransBase, "Submit")
}
