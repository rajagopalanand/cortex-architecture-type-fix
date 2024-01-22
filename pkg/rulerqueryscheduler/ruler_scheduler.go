package rulerqueryscheduler

import (
	"context"
	"errors"
	"flag"
	"github.com/cortexproject/cortex/pkg/ring"
	"github.com/cortexproject/cortex/pkg/util"
	"github.com/cortexproject/cortex/pkg/util/services"
	"github.com/go-kit/log"
	"github.com/go-kit/log/level"
	"time"
)

type Config struct {
}

func (cfg *Config) RegisterFlags(f *flag.FlagSet) {

}

type RulerScheduler struct {
	cfg Config
	services.Service
	lifecycler         *ring.BasicLifecycler
	ring               *ring.Ring
	subservices        *services.Manager
	subservicesWatcher *services.FailureWatcher
	logger             log.Logger
}

func NewRulerScheduler(cfg Config, logger log.Logger) (*RulerScheduler, error) {
	rs := &RulerScheduler{
		cfg:    cfg,
		logger: logger,
	}
	rs.Service = services.NewBasicService(rs.starting, rs.run, rs.stopping)
	return rs, nil
}

func (r *RulerScheduler) starting(ctx context.Context) error {
	return nil
}

func (r *RulerScheduler) stopping(_ error) error {
	return nil
}

func (r *RulerScheduler) run(ctx context.Context) error {
	syncTicker := time.NewTicker(util.DurationWithJitter(time.Minute, 0.2))
	for {
		select {
		case <-syncTicker.C:
			level.Info(r.logger).Log("ruler scheduler heartbeat")
		case <-ctx.Done():
			return nil
		}
	}
}

func (r *RulerScheduler) Query(ctx context.Context, in *PromQLInstantQueryRequest) (*PromQLInstantQueryResult, error) {
	level.Info(r.logger).Log("Query called", "query", in.Qs, "ts", in.Ts)
	return nil, errors.New("Not yet implemented")
}
