package telemetry

import (
	"net/http"
	"net/http/httptrace"
	"sync/atomic"
	"time"

	"github.com/prometheus/client_golang/prometheus"
)

func init() {
	// Register metrics with Prometheus
	prometheus.MustRegister(httpInFlight)
	prometheus.MustRegister(httpDuration)
	prometheus.MustRegister(httpQueueTime)
	prometheus.MustRegister(rpcInFlight)
	prometheus.MustRegister(rpcDuration)
}

var (
	// Transport level metrics

	httpInFlight = prometheus.NewGaugeVec(
		prometheus.GaugeOpts{
			Name: "http_client_in_flight_requests",
			Help: "Current number of in-flight HTTP requests",
		},
		[]string{"client"},
	)

	httpDuration = prometheus.NewHistogramVec(
		prometheus.HistogramOpts{
			Name:    "http_client_duration_seconds",
			Help:    "HTTP request duration distributions",
			Buckets: []float64{.005, .01, .025, .05, .1, .25, .5, 1, 2.5, 5},
		},
		[]string{"client", "status"},
	)

	httpQueueTime = prometheus.NewHistogramVec(
		prometheus.HistogramOpts{
			Name:    "http_client_queue_seconds",
			Help:    "Time spent waiting before request starts",
			Buckets: []float64{.001, .005, .01, .025, .05, .1, .25, .5},
		},
		[]string{"client"},
	)

	// RPC level metrics
	rpcInFlight = prometheus.NewGaugeVec(
		prometheus.GaugeOpts{
			Name: "rpc_in_flight_requests",
			Help: "Current number of in-flight RPC requests",
		},
		[]string{"client", "endpoint"},
	)

	rpcDuration = prometheus.NewHistogramVec(
		prometheus.HistogramOpts{
			Name:    "rpc_duration_seconds",
			Help:    "RPC request duration distributions",
			Buckets: []float64{.005, .01, .025, .05, .1, .25, .5, 1, 2.5, 5},
		},
		[]string{"client", "endpoint"},
	)
)

type MetricsTransport struct {
	name     string
	wrapped  http.RoundTripper
	inFlight int64
}

func NewMetricsTransport(name string, wrapped http.RoundTripper) *MetricsTransport {
	if wrapped == nil {
		wrapped = http.DefaultTransport
	}
	return &MetricsTransport{
		name:    name,
		wrapped: wrapped,
	}
}

func (t *MetricsTransport) RoundTrip(req *http.Request) (*http.Response, error) {
	start := time.Now()
	atomic.AddInt64(&t.inFlight, 1)
	defer atomic.AddInt64(&t.inFlight, -1)

	httpInFlight.WithLabelValues(t.name).Set(float64(atomic.LoadInt64(&t.inFlight)))

	trace := &httptrace.ClientTrace{
		GetConn: func(hostPort string) {
			httpQueueTime.WithLabelValues(t.name).Observe(time.Since(start).Seconds())
		},
	}
	req = req.WithContext(httptrace.WithClientTrace(req.Context(), trace))

	resp, err := t.wrapped.RoundTrip(req)
	if err != nil {
		return nil, err
	}

	status := resp.Status
	httpDuration.WithLabelValues(t.name, status).Observe(time.Since(start).Seconds())

	return resp, nil
}
