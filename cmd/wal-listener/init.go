package main

import (
	"context"
	"crypto/tls"
	"fmt"
	"log/slog"

	pgxslog "github.com/induzo/gocom/database/pgx-slog"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/tracelog"
	"github.com/nats-io/nats.go"

	"github.com/ihippik/wal-listener/v2/config"
	"github.com/ihippik/wal-listener/v2/publisher"
)

// initPgxConnections initialise db and replication connections.
func initPgxConnections(ctx context.Context, cfg *config.DatabaseCfg, logger *slog.Logger) (*pgx.Conn, error) {
	connString := fmt.Sprintf("postgres://%s:%s@%s:%d/%s", cfg.User, cfg.Password, cfg.Host, cfg.Port, cfg.Name)
	pgxConf, err := pgx.ParseConfig(connString)
	if err != nil {
		return nil, fmt.Errorf("db config: %w", err)
	}

	pgxConf.Tracer = &tracelog.TraceLog{Logger: pgxslog.NewLogger(logger), LogLevel: tracelog.LogLevelInfo}

	if cfg.SSL != nil {
		pgxConf.TLSConfig = &tls.Config{
			InsecureSkipVerify: cfg.SSL.SkipVerify,
		}
	}

	pgConn, err := pgx.ConnectConfig(ctx, pgxConf)
	if err != nil {
		return nil, fmt.Errorf("db connection: %w", err)
	}

	// rConnection, err := pgx.ReplicationConnect(pgxConf)
	// if err != nil {
	// 	return nil, nil, fmt.Errorf("replication connect: %w", err)
	// }

	return pgConn, nil
}

type eventPublisher interface {
	Publish(context.Context, string, *publisher.Event) publisher.PublishResult
	Flush(string)
	Close() error
}

// factoryPublisher represents a factory function for creating a eventPublisher.
func factoryPublisher(ctx context.Context, cfg *config.PublisherCfg, logger *slog.Logger) (eventPublisher, error) {
	switch cfg.Type {
	case config.PublisherTypeKafka:
		producer, err := publisher.NewProducer(cfg)
		if err != nil {
			return nil, fmt.Errorf("kafka producer: %w", err)
		}

		return publisher.NewKafkaPublisher(producer), nil
	case config.PublisherTypeNats:
		conn, err := nats.Connect(cfg.Address)
		if err != nil {
			return nil, fmt.Errorf("nats connection: %w", err)
		}

		pub, err := publisher.NewNatsPublisher(conn, logger)
		if err != nil {
			return nil, fmt.Errorf("new nats publisher: %w", err)
		}

		if err := pub.CreateStream(cfg.Topic); err != nil {
			return nil, fmt.Errorf("create stream: %w", err)
		}

		return pub, nil
	case config.PublisherTypeRabbitMQ:
		conn, err := publisher.NewConnection(cfg)
		if err != nil {
			return nil, fmt.Errorf("new connection: %w", err)
		}

		p, err := publisher.NewPublisher(cfg.Topic, conn)
		if err != nil {
			return nil, fmt.Errorf("new publisher: %w", err)
		}

		pub, err := publisher.NewRabbitPublisher(cfg.Topic, conn, p)
		if err != nil {
			return nil, fmt.Errorf("new rabbit publisher: %w", err)
		}

		return pub, nil
	case config.PublisherTypeGooglePubSub:
		pubSubConn, err := publisher.NewPubSubConnection(ctx, logger, cfg.PubSubProjectID, cfg.EnableOrdering)
		if err != nil {
			return nil, fmt.Errorf("could not create pubsub connection: %w", err)
		}

		return publisher.NewGooglePubSubPublisher(pubSubConn), nil
	default:
		return nil, fmt.Errorf("unknown publisher type: %s", cfg.Type)
	}
}
