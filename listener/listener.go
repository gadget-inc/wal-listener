package listener

import (
	"context"
	"fmt"
	"log/slog"
	"net/http"
	"os"
	"os/signal"
	"strconv"
	"sync"
	"sync/atomic"
	"time"

	retry "github.com/avast/retry-go/v4"
	"github.com/jackc/pglogrepl"
	"github.com/jackc/pgx/v5"
	"golang.org/x/sync/errgroup"

	"github.com/ihippik/wal-listener/v2/config"
	"github.com/ihippik/wal-listener/v2/publisher"
)

// Logical decoding plugin.
const pgOutputPlugin = "pgoutput"

type eventPublisher interface {
	Publish(context.Context, string, *publisher.Event) publisher.PublishResult
	Flush(string)
}

type parser interface {
	ParseWalMessage([]byte, *WalTransaction) error
}

type replication interface {
	CreateReplicationSlotEx(slotName, outputPlugin string) (consistentPoint string, snapshotName string, err error)
	DropReplicationSlot(slotName string) (err error)
	StartReplication(slotName string, startLsn uint64, timeline int64, pluginArguments ...string) (err error)
	WaitForReplicationMessage(ctx context.Context) (*pgx.ReplicationMessage, error)
	SendStandbyStatus(k *pglogrepl.StandbyStatusUpdate) (err error)
	IsAlive() bool
	Close() error
}

type repository interface {
	CreatePublication(ctx context.Context, name string) error
	GetSlotLSN(ctx context.Context, slotName string) (string, error)
	NewStandbyStatus(lsn pglogrepl.LSN) pglogrepl.StandbyStatusUpdate
	IsClosed() bool
	Close(context.Context) error
}

type monitor interface {
	IncPublishedEvents(subject, table string)
	IncFilterSkippedEvents(table string)
	IncProblematicEvents(kind string)
}

type messageAndEvents struct {
	msg    *pgx.ReplicationMessage
	events []*publisher.Event
}

type eventAndPublishResult struct {
	subjectName string
	event       *publisher.Event
	result      publisher.PublishResult
	// Only include the message on the final event created from it
	// we only ACK after the last message has been successfully published
	msg *pgx.ReplicationMessage
}

// Listener main service struct.
type Listener struct {
	cfg        *config.Config
	log        *slog.Logger
	conn       *pgx.Conn
	publisher  eventPublisher
	parser     parser
	repository repository
	monitor    monitor

	lsn     pglogrepl.LSN
	isAlive atomic.Bool

	// repository repository
	mu sync.RWMutex
}

// NewWalListener create and initialize new service instance.
func NewWalListener(
	cfg *config.Config,
	log *slog.Logger,
	conn *pgx.Conn,
	pub eventPublisher,
	parser parser,
	repository repository,
	monitor monitor,
) *Listener {
	return &Listener{
		cfg:        cfg,
		log:        log,
		conn:       conn,
		publisher:  pub,
		parser:     parser,
		repository: repository,
		monitor:    monitor,
	}
}

// InitHandlers init web handlers for liveness & readiness k8s probes.
func (l *Listener) InitHandlers(ctx context.Context) {
	const defaultTimeout = 500 * time.Millisecond

	if l.cfg.Listener.ServerPort == 0 {
		l.log.Debug("web server port for probes not specified, skip")
		return
	}

	handler := http.NewServeMux()
	handler.HandleFunc("GET /healthz", l.liveness)
	handler.HandleFunc("GET /ready", l.readiness)

	addr := ":" + strconv.Itoa(l.cfg.Listener.ServerPort)
	srv := http.Server{
		Addr:         addr,
		Handler:      handler,
		ReadTimeout:  defaultTimeout,
		WriteTimeout: defaultTimeout,
	}

	go func() {
		if err := srv.ListenAndServe(); err != nil {
			l.log.Error("error starting http listener", "err", err)
		}
	}()

	l.log.Debug("web handlers were initialised", slog.String("addr", addr))

	<-ctx.Done()
}

const contentTypeTextPlain = "text/plain"

func (l *Listener) liveness(w http.ResponseWriter, r *http.Request) {
	var (
		respCode = http.StatusOK
		resp     = []byte(`ok`)
	)

	w.Header().Set("Content-Type", contentTypeTextPlain)

	if l.conn.IsClosed() || l.repository.IsClosed() {
		resp = []byte("failed")
		respCode = http.StatusInternalServerError

		l.log.Warn("liveness probe failed")
	}

	w.WriteHeader(respCode)

	if _, err := w.Write(resp); err != nil {
		l.log.Error("liveness: error writing response", "err", err)
	}
}

func (l *Listener) readiness(w http.ResponseWriter, r *http.Request) {
	var (
		respCode = http.StatusOK
		resp     = []byte(`ok`)
	)

	w.Header().Set("Content-Type", contentTypeTextPlain)

	if !l.isAlive.Load() {
		resp = []byte("failed")
		respCode = http.StatusInternalServerError

		l.log.Warn("readiness probe failed")
	}

	w.WriteHeader(respCode)

	if _, err := w.Write(resp); err != nil {
		l.log.Error("liveness: error writing response", "err", err)
	}
}

// Process is main service entry point.
func (l *Listener) Process(ctx context.Context) error {
	logger := l.log.With("slot_name", l.cfg.Listener.SlotName)

	ctx, stop := signal.NotifyContext(ctx, os.Interrupt)
	defer stop()

	logger.Info("service was started")

	if err := l.repository.CreatePublication(ctx, publicationName); err != nil {
		logger.Warn("publication creation was skipped", "err", err)
	}

	slotIsExists, err := l.slotIsExists(ctx)
	if err != nil {
		return fmt.Errorf("slot is exists: %w", err)
	}

	if !slotIsExists {
		result, err := pglogrepl.CreateReplicationSlot(ctx, l.conn.PgConn(), l.cfg.Listener.SlotName, pgOutputPlugin, pglogrepl.CreateReplicationSlotOptions{})
		if err != nil {
			return fmt.Errorf("create replication slot: %w", err)
		}

		lsn, err := pglogrepl.ParseLSN(result.ConsistentPoint)
		if err != nil {
			return fmt.Errorf("parse lsn: %w", err)
		}

		l.setLSN(lsn)

		logger.Info("new slot was created", slog.String("slot", l.cfg.Listener.SlotName))
	} else {
		logger.Info("slot already exists, LSN updated")
	}

	group, ctx := errgroup.WithContext(ctx)

	group.Go(func() error {
		return l.Stream(ctx)
	})
	group.Go(func() error {
		return l.checkConnection(ctx)
	})

	if err = group.Wait(); err != nil {
		return err
	}

	return nil
}

// checkConnection periodically checks connections.
func (l *Listener) checkConnection(ctx context.Context) error {
	refresh := time.NewTicker(l.cfg.Listener.RefreshConnection)
	defer refresh.Stop()

	for {
		select {
		case <-refresh.C:
			if l.conn.IsClosed() {
				return fmt.Errorf("pg connection: %w", errReplConnectionIsLost)
			}
		case <-ctx.Done():
			l.log.Debug("check connection: context was canceled")

			if err := l.Stop(); err != nil {
				l.log.Error("failed to stop service", "err", err)
			}

			return nil
		}
	}
}

// slotIsExists checks whether a slot has already been created and if it has been created uses it.
func (l *Listener) slotIsExists(ctx context.Context) (bool, error) {
	restartLSNStr, err := l.repository.GetSlotLSN(ctx, l.cfg.Listener.SlotName)
	if err != nil {
		return false, fmt.Errorf("get slot lsn: %w", err)
	}

	if len(restartLSNStr) == 0 {
		l.log.Warn("restart LSN not found", slog.String("slot_name", l.cfg.Listener.SlotName))
		return false, nil
	}

	lsn, err := pglogrepl.ParseLSN(restartLSNStr)
	if err != nil {
		return false, fmt.Errorf("parse lsn: %w", err)
	}

	l.setLSN(lsn)

	return true, nil
}

const (
	protoVersion    = "proto_version '1'"
	publicationName = "wal-listener"
)

const (
	problemKindParse   = "parse"
	problemKindPublish = "publish"
	problemKindAck     = "ack"
)

// Stream receive event from PostgreSQL.
// Accept message, apply filter and  publish it in NATS server.
func (l *Listener) Stream(ctx context.Context) error {
	if err := pglogrepl.StartReplication(
		ctx,
		l.conn.PgConn(),
		l.cfg.Listener.SlotName,
		l.readLSN(),
		pglogrepl.StartReplicationOptions{
			Timeline: -1,
			PluginArgs: []string{
				protoVersion,
				publicationNames(publicationName),
			},
		},
	); err != nil {
		return fmt.Errorf("start replication: %w", err)
	}

	go l.SendPeriodicHeartbeats(ctx)

	pool := &sync.Pool{
		New: func() any {
			return &publisher.Event{}
		},
	}

	topicSet := make(map[string]bool)
	defer func() {
		for topic := range topicSet {
			l.publisher.Flush(topic)
		}
	}()

	tx := NewWalTransaction(l.log, pool, l.monitor, l.cfg.Listener.Include.Tables, l.cfg.Listener.Exclude.Tables)

	group, ctx := errgroup.WithContext(ctx)
	messageChan := make(chan *pgx.ReplicationMessage, 100)
	eventsChan := make(chan *messageAndEvents, 100)
	resultChan := make(chan *eventAndPublishResult, 500)

	defer close(messageChan)
	defer close(eventsChan)
	defer close(resultChan)

	group.Go(func() error {
		for {
			if err := ctx.Err(); err != nil {
				l.log.Warn("stream: context canceled", "err", err)
				return nil
			}

			msg, err := l.replicator.WaitForReplicationMessage(ctx)
			if err != nil {
				return fmt.Errorf("wait for replication message: %w", err)
			}

			if msg == nil || msg.WalMessage == nil {
				continue
			}

			messageChan <- msg
		}
	})

	group.Go(func() error {
		for {
			select {
			case <-ctx.Done():
				l.log.Warn("stream: context canceled", "err", ctx.Err())
				return nil
			case msg := <-messageChan:
				err := l.parser.ParseWalMessage(msg.WalMessage.WalData, tx)
				if err != nil {
					l.monitor.IncProblematicEvents(problemKindParse)
					return fmt.Errorf("parse: %w", err)
				}

				if tx.CommitTime != nil {
					events := tx.CreateEventsWithFilter(ctx)

					eventsChan <- &messageAndEvents{
						msg,
						events,
					}
					tx.Clear()
				}

				l.processHeartBeat(msg)
			}
		}
	})

	group.Go(func() error {
		for {
			select {
			case <-ctx.Done():
				l.log.Warn("stream: context canceled", "err", ctx.Err())
				return nil
			case object := <-eventsChan:
				events, msg := object.events, object.msg

				for idx, event := range events {
					subjectName := event.SubjectName(l.cfg)
					topicSet[subjectName] = true

					result := l.publisher.Publish(ctx, subjectName, event)

					// Include the message on the last event of the batch
					if idx == len(events)-1 {
						resultChan <- &eventAndPublishResult{
							subjectName: subjectName,
							event:       event,
							result:      result,
							msg:         msg,
						}
					} else {
						resultChan <- &eventAndPublishResult{
							subjectName: subjectName,
							event:       event,
							result:      result,
						}
					}
				}
			}
		}
	})

	group.Go(func() error {
		for {
			select {
			case <-ctx.Done():
				l.log.Warn("stream: context canceled", "err", ctx.Err())
				return nil
			case object := <-resultChan:
				subjectName, result, event, msg := object.subjectName, object.result, object.event, object.msg

				_, err := result.Get(ctx)
				if err != nil {
					// Only warn on publish failures, but continue to process and ack messages
					// this runs the risk of losing data, but this it is more important to keep processing WAL messages in the meantime
					l.monitor.IncProblematicEvents(problemKindPublish)
					l.log.Warn("failed to publish message", slog.Any("error", err), slog.String("subjectName", subjectName), slog.String("table", event.Table), slog.String("action", event.Action))
				} else {
					l.monitor.IncPublishedEvents(subjectName, event.Table)
					l.log.Info(
						"event was sent",
						slog.String("subject", subjectName),
						slog.String("action", event.Action),
						slog.String("table", event.Table),
						slog.Uint64("lsn", uint64(l.readLSN())),
					)
				}

				tx.pool.Put(event)

				if msg != nil {
					if msg.WalMessage.WalStart > l.readLSN() {
						if err := l.AckWalMessage(ctx, msg.WalMessage.WalStart); err != nil {
							l.monitor.IncProblematicEvents(problemKindAck)
							return fmt.Errorf("ack: %w", err)
						}

						l.log.Debug("ack WAL message", slog.Uint64("lsn", uint64(l.readLSN())))
					}
				}
			}

		}
	})

	return group.Wait()
}

func (l *Listener) processHeartBeat(ctx context.Context, msg *pgx.ReplicationMessage) {
	if msg.ServerHeartbeat == nil {
		return
	}

	l.log.Debug(
		"received server heartbeat",
		slog.Uint64("server_wal_end", msg.ServerHeartbeat.ServerWalEnd),
		slog.Uint64("server_time", msg.ServerHeartbeat.ServerTime),
	)

	if msg.ServerHeartbeat.ReplyRequested == 1 {
		l.log.Debug("status requested")

		if err := l.SendStandbyStatus(ctx); err != nil {
			l.log.Warn("send standby status: %w", err)
		}
	}
}

func publicationNames(publication string) string {
	return fmt.Sprintf(`publication_names '%s'`, publication)
}

// Stop is a finalizer function.
func (l *Listener) Stop() error {
	ctx := context.Background()
	if err := l.repository.Close(ctx); err != nil {
		return fmt.Errorf("repository close: %w", err)
	}

	if err := l.conn.Close(ctx); err != nil {
		return fmt.Errorf("pgconn close: %w", err)
	}

	l.log.Info("service was stopped")

	return nil
}

// SendPeriodicHeartbeats send periodic keep alive heartbeats to the server.
func (l *Listener) SendPeriodicHeartbeats(ctx context.Context) {
	heart := time.NewTicker(l.cfg.Listener.HeartbeatInterval)
	defer heart.Stop()

	for {
		select {
		case <-ctx.Done():
			l.log.Warn("periodic heartbeats: context was canceled")
			return
		case <-heart.C:
			if err := l.SendStandbyStatus(ctx); err != nil {
				l.log.Error("failed to send heartbeat status", "err", err)
				l.isAlive.Store(false)

				continue
			}

			l.isAlive.Store(true)
			l.log.Debug("sending periodic heartbeat status")
		}
	}
}

// SendStandbyStatus sends a `StandbyStatus` object with the current RestartLSN value to the server.
func (l *Listener) SendStandbyStatus(ctx context.Context) error {
	lsn := l.readLSN()

	return retry.Do(func() error {
		standbyStatus := l.repository.NewStandbyStatus(lsn)

		if err := pglogrepl.SendStandbyStatusUpdate(ctx, l.conn.PgConn(), standbyStatus); err != nil {
			return fmt.Errorf("unable to send StandbyStatus object: %w", err)
		}

		return nil
	}, retry.Attempts(3))
}

// AckWalMessage acknowledge received wal message.
func (l *Listener) AckWalMessage(ctx context.Context, lsn pglogrepl.LSN) error {
	l.setLSN(lsn)

	if err := l.SendStandbyStatus(ctx); err != nil {
		return fmt.Errorf("send status: %w", err)
	}

	return nil
}

func (l *Listener) readLSN() pglogrepl.LSN {
	l.mu.RLock()
	defer l.mu.RUnlock()

	return l.lsn
}

func (l *Listener) setLSN(lsn pglogrepl.LSN) {
	l.mu.Lock()
	defer l.mu.Unlock()

	l.lsn = lsn
}
