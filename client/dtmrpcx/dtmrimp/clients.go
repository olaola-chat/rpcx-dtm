package dtmrimp

import (
	"errors"
	"fmt"
	"github.com/dtm-labs/dtm/client/dtmrpcx/dtmcli"
	"github.com/dtm-labs/dtm/dtmsvr/config"
	"net/url"
	"sync"

	"github.com/dtm-labs/dtm/client/dtmcli/dtmimp"
	_ "github.com/gogf/gf"
	rpcXClient "github.com/smallnest/rpcx/client"
)

var (
	xClients            sync.Map
	consulDiscovery     rpcXClient.ServiceDiscovery
	onceConsulDiscovery sync.Once
)

type discoverConfig struct {
}

func getConsulDiscovery() rpcXClient.ServiceDiscovery {
	onceConsulDiscovery.Do(func() {
		if consulDiscovery == nil {
			if consulDiscovery == nil {
				target := config.Config.MicroService.Target
				if target == "" {
					panic("MicroService.Target is not set")
				}
				uri, err := url.ParseRequestURI(target)
				if err != nil {
					dtmimp.E2P(err)
				}

				if uri.Scheme != "rpcx" {
					panic("MicroService.Target must be rpcx://")
				}

				host, port, path := uri.Hostname(), uri.Port(), uri.Path

				consulDiscovery, err = createServiceDiscovery(fmt.Sprintf("%s:%d", host, port), path)
				dtmimp.E2P(err)
			}
		}
	})
	return consulDiscovery
}

func createServiceDiscovery(regAddr, basePath string) (rpcXClient.ServiceDiscovery, error) {
	return rpcXClient.NewConsulDiscoveryTemplate(basePath, []string{regAddr}, nil)
}

// MustGetDtmRpcXClient 1
func MustGetDtmRpcXClient(rpcXServer string) dtmcli.Client {
	return dtmcli.NewRpcXClient(MustGetRpcXClient(rpcXServer))
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
func MustGetRpcXClient(rpcXServer string) rpcXClient.XClient {
	cli, err := GetRpcXClient(rpcXServer, getConsulDiscovery())
	dtmimp.E2P(err)
	return cli
}
