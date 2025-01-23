package httpu

import (
	"net"
	"net/http"
	"time"

	"reduction.dev/reduction/telemetry"
)

// Create a new http.Client that matches the default http client.
//
// Leaving the client `Transport` field nil results in reusing the
// http.DefaultTransport between clients. In tests this resulted in "new"
// connections that http.Server.Shutdown() could never close. Using a different
// instance of Transport and DialContext seems to avoid the issue.
func NewClient(metricName string) *http.Client {
	transport := &http.Transport{
		DialContext: (&net.Dialer{
			Timeout:   30 * time.Second,
			KeepAlive: 30 * time.Second,
		}).DialContext,
		ForceAttemptHTTP2:     true,
		MaxIdleConns:          100,
		IdleConnTimeout:       90 * time.Second,
		TLSHandshakeTimeout:   10 * time.Second,
		ExpectContinueTimeout: 1 * time.Second,
	}

	return &http.Client{
		Transport: telemetry.NewMetricsTransport(metricName, transport),
	}
}
