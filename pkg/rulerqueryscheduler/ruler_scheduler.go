package rulerqueryscheduler

import (
	"context"
	"flag"
	"github.com/cortexproject/cortex/pkg/ring"
	"github.com/cortexproject/cortex/pkg/ring/kv"
	"github.com/cortexproject/cortex/pkg/util"
	"github.com/cortexproject/cortex/pkg/util/services"
	"github.com/go-kit/log"
	"github.com/go-kit/log/level"
	"github.com/pkg/errors"
	"github.com/prometheus/client_golang/prometheus"
	"io"
	"net/http"
	"time"
)

var (
	ringKey = "ring"
)

type Config struct {
	Ring            RingConfig    `yaml:"ring"`
	RingCheckPeriod time.Duration `yaml:"-"`
}

func (cfg *Config) RegisterFlags(f *flag.FlagSet) {
	cfg.RingCheckPeriod = 5 * time.Second
	cfg.Ring.RegisterFlags(f)
}

type RulerScheduler struct {
	services.Service

	cfg                Config
	lifecycler         *ring.BasicLifecycler
	ring               *ring.Ring
	subservices        *services.Manager
	subservicesWatcher *services.FailureWatcher

	registry prometheus.Registerer
	logger   log.Logger
}

func NewRulerScheduler(cfg Config, reg prometheus.Registerer, logger log.Logger) (*RulerScheduler, error) {
	rs := &RulerScheduler{
		cfg:      cfg,
		logger:   logger,
		registry: reg,
	}
	level.Info(rs.logger).Log("Initializing ruler-scheduler")
	ringStore, err := kv.NewClient(
		cfg.Ring.KVStore,
		ring.GetCodec(),
		kv.RegistererWithKVName(prometheus.WrapRegistererWithPrefix("cortex_", reg), "ruler-scheduler"),
		logger,
	)
	if err != nil {
		return nil, errors.Wrap(err, "create KV store client")
	}

	lifecyclerCfg, err := rs.cfg.Ring.ToLifecyclerConfig(rs.logger)
	if err != nil {
		return nil, errors.Wrap(err, "failed to initialize ruler scheduler's lifecycler config")
	}
	delegate := ring.BasicLifecyclerDelegate(rs)
	delegate = ring.NewLeaveOnStoppingDelegate(delegate, rs.logger)
	delegate = ring.NewAutoForgetDelegate(rs.cfg.Ring.HeartbeatTimeout*ringAutoForgetUnhealthyPeriods, delegate, rs.logger)

	ringName := "ruler-scheduler"
	rs.lifecycler, err = ring.NewBasicLifecycler(lifecyclerCfg, ringName, ringKey, ringStore, delegate, rs.logger, prometheus.WrapRegistererWithPrefix("cortex_", rs.registry))
	if err != nil {
		return nil, errors.Wrap(err, "failed to initialize ruler scheduler's lifecycler")
	}

	rs.ring, err = ring.NewWithStoreClientAndStrategy(rs.cfg.Ring.ToRingConfig(), ringName, ringKey, ringStore, ring.NewIgnoreUnhealthyInstancesReplicationStrategy(), prometheus.WrapRegistererWithPrefix("cortex_", rs.registry), rs.logger)
	if err != nil {
		return nil, errors.Wrap(err, "failed to initialize ruler scheduler's ring")
	}

	rs.Service = services.NewBasicService(rs.starting, rs.run, rs.stopping)
	return rs, nil
}

func (r *RulerScheduler) starting(ctx context.Context) error {
	var err error

	if r.subservices, err = services.NewManager(r.lifecycler, r.ring); err != nil {
		return errors.Wrap(err, "unable to start ruler sub-services")
	}

	r.subservicesWatcher = services.NewFailureWatcher()
	r.subservicesWatcher.WatchManager(r.subservices)

	if err = services.StartManagerAndAwaitHealthy(ctx, r.subservices); err != nil {
		return errors.Wrap(err, "unable to start ruler scheduler's sub-services")
	}
	return nil
}

func (r *RulerScheduler) stopping(_ error) error {
	if r.subservices != nil {
		_ = services.StopManagerAndAwaitStopped(context.Background(), r.subservices)
	}
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

func (r *RulerScheduler) ServeHTTP(w http.ResponseWriter, req *http.Request) {
	r.ring.ServeHTTP(w, req)
}

func (r *RulerScheduler) Enqueue(srv SchedulerForRuler_EnqueueServer) error {
	if r.State() == services.Running {
		err := srv.Send(&ScheduleEvaluationResponse{
			Status: OK,
		})
		if err != nil {
			return err
		}
	}
	for r.State() == services.Running {
		msg, err := srv.Recv()
		if err != nil {
			if err == io.EOF {
				return nil
			}
			return err
		}
		if r.State() != services.Running {
			break // break out of the loop, and send SHUTTING_DOWN message.
		}
		var resp *ScheduleEvaluationResponse
		switch msg.Type {
		case ENQUEUE:
			level.Info(r.logger).Log("msg", "received eval request", "rule group", msg.RuleGroup, "user", msg.UserID)
			resp = &ScheduleEvaluationResponse{
				Status: OK,
			}
		}
		err = srv.Send(resp)
		if err != nil {
			return err
		}
	}
	return srv.Send(&ScheduleEvaluationResponse{
		Status: SHUTTING_DOWN,
	})
}
