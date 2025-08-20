package driver

import (
	"fmt"
	"github.com/dtm-labs/dtmdriver"
	"net/url"
	"strings"
)

const (
	DriverName = "dtm-driver-slp"
)

type slpRpcxDriver struct {
}

// GetName return the name of the driver
func (srd *slpRpcxDriver) GetName() string {
	return ""
}

// RegisterAddrResolver will be called when driver used
// for gRPC: register grpc resolver
// for HTTP: add your http middleware
func (srd *slpRpcxDriver) RegisterAddrResolver() {}

// RegisterService register dtm endpoint to target.
// for both http and grpc
func (srd *slpRpcxDriver) RegisterService(target string, endpoint string) error { return nil }

func (srd *slpRpcxDriver) ParseServerMethod(uri string) (server string, method string, err error) {
	if !strings.Contains(uri, "//") { // 处理无scheme的情况，如果您没有直连，可以不处理
		sep := strings.IndexByte(uri, '/')
		if sep == -1 {
			return "", "", fmt.Errorf("bad url: '%s'. no '/' found", uri)
		}
		return uri[:sep], uri[sep:], nil

	}
	//resolve gozero consul wait=xx url.Parse no standard
	if (strings.Contains(uri, kindConsul) || strings.Contains(uri, kindNacos)) && strings.Contains(uri, "?") {
		tmp := strings.Split(uri, "?")
		sep := strings.IndexByte(tmp[1], '/')
		if sep == -1 {
			return "", "", fmt.Errorf("bad url: '%s'. no '/' found", uri)
		}
		uri = tmp[0] + tmp[1][sep:]
	}

	u, err := url.Parse(uri)
	if err != nil {
		return "", "", nil
	}
	index := strings.IndexByte(u.Path[1:], '/') + 1

	return u.Scheme + "://" + u.Host + u.Path[:index], u.Path[index:], nil
}

func init() {
	dtmdriver.Register(&zeroDriver{})
}
