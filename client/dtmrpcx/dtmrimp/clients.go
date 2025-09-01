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
	xClients             sync.Map
	consulDiscovery      rpcXClient.ServiceDiscovery
	onceConsulDiscovery  sync.Once
	consulConfig         *ConsulConfig
	onceLoadConsulConfig sync.Once
)

type discoverConfig struct {
}

type ConsulConfig struct {
	Address string
	Prefix  string
}

func GetConsulConfig() *ConsulConfig {
	if consulConfig != nil {
		return consulConfig
	}
	onceLoadConsulConfig.Do(func() {
		if consulDiscovery == nil {
			target := config.Config.MicroService.Target
			targetUrl, err := url.Parse(target)
			dtmimp.E2P(err)
			if targetUrl.Scheme != "consul" {
				dtmimp.E2P(fmt.Errorf("unknown scheme: %s", targetUrl.Scheme))
			}
			consulConfig = &ConsulConfig{
				Address: fmt.Sprintf(fmt.Sprintf("%s", targetUrl.Host)),
				Prefix:  targetUrl.Path,
			}
		}
	})
	return consulConfig
}

func GetConsulDiscovery() rpcXClient.ServiceDiscovery {
	cc := GetConsulConfig()
	onceConsulDiscovery.Do(func() {
		if consulDiscovery == nil {
			if consulDiscovery == nil {
				var err error
				consulDiscovery, err = createServiceDiscovery(cc.Address, cc.Prefix)
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
	cli, err := GetRpcXClient(rpcXServer, GetConsulDiscovery())
	dtmimp.E2P(err)
	return cli
}
