package driver

import (
	"fmt"
	"github.com/dtm-labs/dtmdriver"
	"net/url"
	"strings"
)

const (
	DriverName = "dtm-driver-rpcx"
	kindEtcd   = "etcd"
	kindDiscov = "discov"
	kindConsul = "consul"
	kindNacos  = "nacos"
)

type slpRpcxDriver struct {
}

// GetName return the name of the driver
func (srd *slpRpcxDriver) GetName() string {
	return DriverName
}

// RegisterAddrResolver will be called when driver used
// for gRPC: register grpc resolver
// for HTTP: add your http middleware
func (srd *slpRpcxDriver) RegisterAddrResolver() {}

// RegisterService register dtm endpoint to target.
// for both http and grpc
func (srd *slpRpcxDriver) RegisterService(target string, endpoint string) error { return nil }

func (srd *slpRpcxDriver) ParseServerMethod(uri string) (server string, method string, err error) {
	uriObj, err := url.Parse(uri)
	if err != nil {
		return "", "", err
	}

	paths := strings.Split(uriObj.Path, "/")
	pathList := make([]string, 0)
	for _, path := range paths {
		if path != "" {
			pathList = append(pathList, path)
		}
	}

	if len(pathList) < 2 {
		return "", "", fmt.Errorf("invalid uri: %s", uri)
	}

	return pathList[0], pathList[1], nil
}

func init() {
	dtmdriver.Register(&slpRpcxDriver{})
}
