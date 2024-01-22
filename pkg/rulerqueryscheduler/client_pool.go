package rulerqueryscheduler

import (
	"time"

	"github.com/go-kit/log"
	"github.com/pkg/errors"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
	"google.golang.org/grpc"
	"google.golang.org/grpc/health/grpc_health_v1"

	"github.com/cortexproject/cortex/pkg/ring/client"
	"github.com/cortexproject/cortex/pkg/util/grpcclient"
	"github.com/cortexproject/cortex/pkg/util/services"
)

// ClientsPool is the interface used to get the client from the pool for a specified address.
type ClientsPool interface {
	services.Service
	// GetClientFor returns the ruler client for the given address.
	GetClientFor(addr string) (RulerSchedulerClient, error)
}

type rulerSchedulerClientsPool struct {
	*client.Pool
}

func (p *rulerSchedulerClientsPool) GetClientFor(addr string) (RulerSchedulerClient, error) {
	c, err := p.Pool.GetClientFor(addr)
	if err != nil {
		return nil, err
	}
	return c.(RulerSchedulerClient), nil
}

func NewRulerSchedulerClientPool(clientCfg grpcclient.Config, logger log.Logger, reg prometheus.Registerer) ClientsPool {
	// We prefer sane defaults instead of exposing further config options.
	poolCfg := client.PoolConfig{
		CheckInterval:      time.Minute,
		HealthCheckEnabled: true,
		HealthCheckTimeout: 10 * time.Second,
	}

	clientsCount := promauto.With(reg).NewGauge(prometheus.GaugeOpts{
		Name: "cortex_ruler_scheduler_clients",
		Help: "The current number of ruler clients in the pool.",
	})

	return &rulerSchedulerClientsPool{
		client.NewPool("rulerscheduler", poolCfg, nil, newRulerSchedulerClientFactory(clientCfg, reg), clientsCount, logger),
	}
}

func newRulerSchedulerClientFactory(clientCfg grpcclient.Config, reg prometheus.Registerer) client.PoolFactory {
	requestDuration := promauto.With(reg).NewHistogramVec(prometheus.HistogramOpts{
		Name:    "cortex_ruler_scheduler_client_request_duration_seconds",
		Help:    "Time spent executing requests to the ruler scheduler.",
		Buckets: prometheus.ExponentialBuckets(0.008, 4, 7),
	}, []string{"operation", "status_code"})

	return func(addr string) (client.PoolClient, error) {
		return dialRulerSchedulerClient(clientCfg, addr, requestDuration)
	}
}

func dialRulerSchedulerClient(clientCfg grpcclient.Config, addr string, requestDuration *prometheus.HistogramVec) (*rulerSchedulerExtendedClient, error) {
	opts, err := clientCfg.DialOption(grpcclient.Instrument(requestDuration))
	if err != nil {
		return nil, err
	}

	conn, err := grpc.Dial(addr, opts...)
	if err != nil {
		return nil, errors.Wrapf(err, "failed to dial ruler %s", addr)
	}

	return &rulerSchedulerExtendedClient{
		RulerSchedulerClient: NewRulerSchedulerClient(conn),
		HealthClient:         grpc_health_v1.NewHealthClient(conn),
		conn:                 conn,
	}, nil
}

type rulerSchedulerExtendedClient struct {
	RulerSchedulerClient
	grpc_health_v1.HealthClient
	conn *grpc.ClientConn
}

func (c *rulerSchedulerExtendedClient) Close() error {
	return c.conn.Close()
}

func (c *rulerSchedulerExtendedClient) String() string {
	return c.RemoteAddress()
}

func (c *rulerSchedulerExtendedClient) RemoteAddress() string {
	return c.conn.Target()
}
