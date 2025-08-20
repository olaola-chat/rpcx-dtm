package dtmrimp

import (
	"errors"
	"github.com/dtm-labs/dtm/client/dtmrpcx/dtmcli"
	"sync"

	"github.com/dtm-labs/dtm/client/dtmcli/dtmimp"
	rpcXClient "github.com/smallnest/rpcx/client"
)

var (
	xClients            sync.Map
	consulDiscovery     rpcXClient.ServiceDiscovery
	onceConsulDiscovery sync.Once
)

func getConsulDiscovery(regAddr, basePath string) rpcXClient.ServiceDiscovery {
	if consulDiscovery != nil {
		return consulDiscovery
	}
	onceConsulDiscovery.Do(func() {
		if consulDiscovery == nil {
			var err error
			consulDiscovery, err = createServiceDiscovery(regAddr, basePath)
			dtmimp.E2P(err)
		}
	})
	return consulDiscovery
}

func createServiceDiscovery(regAddr, basePath string) (rpcXClient.ServiceDiscovery, error) {
	return rpcXClient.NewConsulDiscoveryTemplate(basePath, []string{regAddr}, nil)
}

// MustGetDtmRpcXClient 1
func MustGetDtmRpcXClient(rpcXServer string, discovery rpcXClient.ServiceDiscovery) dtmcli.Client {
	return dtmcli.NewRpcXClient(MustGetRpcXClient(rpcXServer, discovery))
}

// GetRpcXClient 1
func GetRpcXClient(rpcXServer string, discov rpcXClient.ServiceDiscovery) (rpcXClient.XClient, error) {
	if srv, ok := xClients.Load(rpcXServer); ok && srv != nil {
		c, ok := srv.(rpcXClient.XClient)
		if !ok || c == nil {
			return nil, errors.New("nil client")
		}
		return c, nil
	}
	c := rpcXClient.NewXClient(rpcXServer, rpcXClient.Failtry, rpcXClient.RoundRobin, discov, rpcXClient.DefaultOption)

	xClients.Store(rpcXServer, c)
	return c, nil
}

// MustGetRpcXClient 1
func MustGetRpcXClient(rpcXServer string, discov rpcXClient.ServiceDiscovery) rpcXClient.XClient {
	cli, err := GetRpcXClient(rpcXServer, discov)
	dtmimp.E2P(err)
	return cli
}
