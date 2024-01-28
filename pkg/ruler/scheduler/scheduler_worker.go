package scheduler

import (
	"context"
	"flag"
	"github.com/cortexproject/cortex/pkg/rulerqueryscheduler"
	"github.com/cortexproject/cortex/pkg/util/flagext"
	"github.com/cortexproject/cortex/pkg/util/grpcclient"
	"sync"
	"time"

	"github.com/go-kit/log"
	"github.com/go-kit/log/level"
	"github.com/pkg/errors"
	"google.golang.org/grpc"

	"github.com/cortexproject/cortex/pkg/util"
	"github.com/cortexproject/cortex/pkg/util/backoff"
	"github.com/cortexproject/cortex/pkg/util/services"
)

type Config struct {
	SchedulerAddress                  string            `yaml:"scheduler_address"`
	DNSLookupPeriod                   time.Duration     `yaml:"scheduler_dns_lookup_period"`
	WorkerConcurrency                 int               `yaml:"scheduler_worker_concurrency"`
	GRPCClientConfig                  grpcclient.Config `yaml:"grpc_client_config"`
	RetryOnTooManyOutstandingRequests bool              `yaml:"retry_on_too_many_outstanding_requests"`

	// Used to find local IP address, that is sent to scheduler and querier-worker.
	InfNames []string `yaml:"instance_interface_names"`

	// If set, address is not computed from interfaces.
	Addr string `yaml:"address" doc:"hidden"`
	Port int    `doc:"hidden"`
}

func (cfg *Config) RegisterFlags(f *flag.FlagSet) {
	f.StringVar(&cfg.SchedulerAddress, "ruler.scheduler.scheduler-address", "", "DNS hostname used for finding query-schedulers.")
	f.DurationVar(&cfg.DNSLookupPeriod, "ruler.scheduler.scheduler-dns-lookup-period", 10*time.Second, "How often to resolve the scheduler-address, in order to look for new query-scheduler instances.")
	f.IntVar(&cfg.WorkerConcurrency, "ruler.scheduler.scheduler-worker-concurrency", 5, "Number of concurrent workers forwarding queries to single query-scheduler.")
	f.BoolVar(&cfg.RetryOnTooManyOutstandingRequests, "ruler.scheduler.retry-on-too-many-outstanding-requests", false, "When multiple query-schedulers are available, re-enqueue queries that were rejected due to too many outstanding requests.")

	cfg.InfNames = []string{"eth0", "en0"}
	f.Var((*flagext.StringSlice)(&cfg.InfNames), "ruler.scheduler.instance-interface-names", "Name of network interface to read address from. This address is sent to query-scheduler and querier, which uses it to send the query response back to query-frontend.")
	f.StringVar(&cfg.Addr, "ruler.scheduler.instance-addr", "", "IP address to advertise to querier (via scheduler) (resolved via interfaces by default).")
	f.IntVar(&cfg.Port, "ruler.scheduler.instance-port", 0, "Port to advertise to querier (via scheduler) (defaults to server.grpc-listen-port).")

	cfg.GRPCClientConfig.RegisterFlagsWithPrefix("ruler.scheduler.grpc-client-config", f)
}

type RulerSchedulerWorkers struct {
	services.Service

	cfg          Config
	log          log.Logger
	rulerAddress string

	// Channel with requests that should be forwarded to the scheduler.
	requestsCh <-chan *RuleSchedulerRequest

	watcher services.Service

	mu sync.Mutex
	// Set to nil when stop is called... no more workers are created afterwards.
	workers map[string]*rulerSchedulerWorker
}

func NewRulerSchedulerWorkers(cfg Config, rulerAddress string, requestsCh <-chan *RuleSchedulerRequest, log log.Logger) (*RulerSchedulerWorkers, error) {
	f := &RulerSchedulerWorkers{
		cfg:          cfg,
		log:          log,
		rulerAddress: rulerAddress,
		requestsCh:   requestsCh,
		workers:      map[string]*rulerSchedulerWorker{},
	}

	w, err := util.NewDNSWatcher(cfg.SchedulerAddress, cfg.DNSLookupPeriod, f)
	if err != nil {
		return nil, err
	}

	f.watcher = w
	f.Service = services.NewIdleService(f.starting, f.stopping)
	return f, nil
}

func (f *RulerSchedulerWorkers) starting(ctx context.Context) error {
	return services.StartAndAwaitRunning(ctx, f.watcher)
}

func (f *RulerSchedulerWorkers) stopping(_ error) error {
	err := services.StopAndAwaitTerminated(context.Background(), f.watcher)

	f.mu.Lock()
	defer f.mu.Unlock()

	for _, w := range f.workers {
		w.stop()
	}
	f.workers = nil

	return err
}

func (f *RulerSchedulerWorkers) AddressAdded(address string) {
	f.mu.Lock()
	ws := f.workers
	w := f.workers[address]
	f.mu.Unlock()

	// Already stopped or we already have worker for this address.
	if ws == nil || w != nil {
		return
	}

	level.Info(f.log).Log("msg", "adding connection to scheduler", "addr", address)
	conn, err := f.connectToScheduler(context.Background(), address)
	if err != nil {
		level.Error(f.log).Log("msg", "error connecting to scheduler", "addr", address, "err", err)
		return
	}

	// No worker for this address yet, start a new one.
	w = newRulerSchedulerWorker(conn, address, f.rulerAddress, f.requestsCh, f.cfg.WorkerConcurrency, f.log)

	f.mu.Lock()
	defer f.mu.Unlock()

	// Can be nil if stopping has been called already.
	if f.workers != nil {
		f.workers[address] = w
		w.start()
	}
}

func (f *RulerSchedulerWorkers) AddressRemoved(address string) {
	level.Info(f.log).Log("msg", "removing connection to scheduler", "addr", address)

	f.mu.Lock()
	// This works fine if f.workers is nil already.
	w := f.workers[address]
	delete(f.workers, address)
	f.mu.Unlock()

	if w != nil {
		w.stop()
	}
}

// Get number of workers.
func (f *RulerSchedulerWorkers) getWorkersCount() int {
	f.mu.Lock()
	defer f.mu.Unlock()

	return len(f.workers)
}

func (f *RulerSchedulerWorkers) connectToScheduler(ctx context.Context, address string) (*grpc.ClientConn, error) {
	// Because we only use single long-running method, it doesn't make sense to inject user ID, send over tracing or add metrics.
	opts, err := f.cfg.GRPCClientConfig.DialOption(nil, nil)
	if err != nil {
		return nil, err
	}

	conn, err := grpc.DialContext(ctx, address, opts...)
	if err != nil {
		return nil, err
	}
	return conn, nil
}

// Worker managing single gRPC connection to Scheduler. Each worker starts multiple goroutines for forwarding
// requests and cancellations to scheduler.
type rulerSchedulerWorker struct {
	log log.Logger

	conn          *grpc.ClientConn
	concurrency   int
	schedulerAddr string
	rulerAddr     string

	// Context and cancellation used by individual goroutines.
	ctx    context.Context
	cancel context.CancelFunc
	wg     sync.WaitGroup

	// Shared between all frontend workers.
	requestCh <-chan *RuleSchedulerRequest

	// Cancellation requests for this scheduler are received via this channel. It is passed to frontend after
	// query has been enqueued to scheduler.
	cancelCh chan uint64
}

func newRulerSchedulerWorker(conn *grpc.ClientConn, schedulerAddr string, rulerAddr string, requestCh <-chan *RuleSchedulerRequest, concurrency int, log log.Logger) *rulerSchedulerWorker {
	w := &rulerSchedulerWorker{
		log:           log,
		conn:          conn,
		concurrency:   concurrency,
		schedulerAddr: schedulerAddr,
		rulerAddr:     rulerAddr,
		requestCh:     requestCh,
		cancelCh:      make(chan uint64, 1000), // Use buffered channel to make sure we can always enqueue to cancel request context.
	}
	w.ctx, w.cancel = context.WithCancel(context.Background())

	return w
}

func (w *rulerSchedulerWorker) start() {
	client := rulerqueryscheduler.NewSchedulerForRulerClient(w.conn)
	for i := 0; i < w.concurrency; i++ {
		w.wg.Add(1)
		go func() {
			defer w.wg.Done()
			w.runOne(w.ctx, client)
		}()
	}
}

func (w *rulerSchedulerWorker) stop() {
	w.cancel()
	w.wg.Wait()
	if err := w.conn.Close(); err != nil {
		level.Error(w.log).Log("msg", "error while closing connection to scheduler", "err", err)
	}
}

func (w *rulerSchedulerWorker) runOne(ctx context.Context, client rulerqueryscheduler.SchedulerForRulerClient) {
	backoffConfig := backoff.Config{
		MinBackoff: 50 * time.Millisecond,
		MaxBackoff: 1 * time.Second,
	}

	backoff := backoff.New(ctx, backoffConfig)
	for backoff.Ongoing() {
		loop, loopErr := client.Enqueue(ctx)
		if loopErr != nil {
			level.Error(w.log).Log("msg", "error contacting scheduler", "err", loopErr, "addr", w.schedulerAddr)
			backoff.Wait()
			continue
		}

		loopErr = w.schedulerLoop(loop)
		if closeErr := loop.CloseSend(); closeErr != nil {
			level.Debug(w.log).Log("msg", "failed to close frontend loop", "err", loopErr, "addr", w.schedulerAddr)
		}

		if loopErr != nil {
			level.Error(w.log).Log("msg", "error sending requests to scheduler", "err", loopErr, "addr", w.schedulerAddr)
			backoff.Wait()
			continue
		}

		backoff.Reset()
	}
}

func (w *rulerSchedulerWorker) schedulerLoop(loop rulerqueryscheduler.SchedulerForRuler_EnqueueClient) error {
	if err := loop.Send(&rulerqueryscheduler.ScheduleEvaluationRequest{
		Type: rulerqueryscheduler.INIT,
	}); err != nil {
		return err
	}

	if resp, err := loop.Recv(); err != nil || resp.Status != rulerqueryscheduler.OK {
		if err != nil {
			return err
		}
		return errors.Errorf("unexpected status received for init: %v", resp.Status)
	}

	ctx := loop.Context()

	for {
		select {
		case <-ctx.Done():
			// No need to report error if our internal context is canceled. This can happen during shutdown,
			// or when scheduler is no longer resolvable. (It would be nice if this context reported "done" also when
			// connection scheduler stops the call, but that doesn't seem to be the case).
			//
			// Reporting error here would delay reopening the stream (if the worker context is not done yet).
			level.Debug(w.log).Log("msg", "stream context finished", "err", ctx.Err())
			return nil

		case req := <-w.requestCh:
			err := loop.Send(&rulerqueryscheduler.ScheduleEvaluationRequest{
				Type:          rulerqueryscheduler.ENQUEUE,
				UserID:        req.UserID,
				Namespace:     req.Namespace,
				RuleGroup:     req.Rulegroup,
				EvalTimestamp: req.EvalTimestamp,
			})

			if err != nil {
				//req.enqueue <- enqueueResult{status: failed}
				level.Error(w.log).Log("msg", "An error occurred while sending request to scheduler", "err", err.Error())
				return err
			}

			resp, err := loop.Recv()
			if err != nil {
				level.Error(w.log).Log("msg", "An error was received from scheduler", "err", err.Error())
				//req.enqueue <- enqueueResult{status: failed}
				return err
			}

			switch resp.Status {
			case rulerqueryscheduler.OK:
				//req.enqueue <- enqueueResult{status: waitForResponse, cancelCh: w.cancelCh}
				// Response will come from querier.
				level.Info(w.log).Log("msg", "Eval message was accepted by scheduler")
			case rulerqueryscheduler.SHUTTING_DOWN:
				// Scheduler is shutting down, report failure to enqueue and stop this loop.
				//req.enqueue <- enqueueResult{status: failed}
				return errors.New("scheduler is shutting down")

			case rulerqueryscheduler.ERROR:
				level.Error(w.log).Log("msg", "An error was received from scheduler")
				//req.enqueue <- enqueueResult{status: waitForResponse}
				//req.response <- &frontendv2pb.QueryResultRequest{
				//	HttpResponse: &httpgrpc.HTTPResponse{
				//		Code: http.StatusInternalServerError,
				//		Body: []byte(err.Error()),
				//	},
				//}

			case rulerqueryscheduler.TOO_MANY_REQUESTS_PER_TENANT:
				level.Error(w.log).Log("msg", "Scheduler returned too many requests per tenant")
				//if req.retryOnTooManyOutstandingRequests {
				//	req.enqueue <- enqueueResult{status: failed}
				//} else {
				//	req.enqueue <- enqueueResult{status: waitForResponse}
				//	req.response <- &frontendv2pb.QueryResultRequest{
				//		HttpResponse: &httpgrpc.HTTPResponse{
				//			Code: http.StatusTooManyRequests,
				//			Body: []byte("too many outstanding requests"),
				//		},
				//	}
				//}
			}

			//case reqID := <-w.cancelCh:
			//err := loop.Send(&schedulerpb.FrontendToScheduler{
			//	Type:    schedulerpb.CANCEL,
			//	QueryID: reqID,
			//})
			//
			//if err != nil {
			//	return err
			//}
			//
			//resp, err := loop.Recv()
			//if err != nil {
			//	return err
			//}
			//
			//// Scheduler may be shutting down, report that.
			//if resp.Status != schedulerpb.OK {
			//	return errors.Errorf("unexpected status received for cancellation: %v", resp.Status)
			//}
		}
	}
}
