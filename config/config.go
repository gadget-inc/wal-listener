package config

import (
	"fmt"
	"os"
	"reflect"
	"regexp"
	"time"

	"github.com/asaskevich/govalidator"
	"github.com/mitchellh/mapstructure"
	"github.com/spf13/viper"

	cfg "github.com/ihippik/config"
)

type PublisherType string

const (
	PublisherTypeNats         PublisherType = "nats"
	PublisherTypeKafka        PublisherType = "kafka"
	PublisherTypeRabbitMQ     PublisherType = "rabbitmq"
	PublisherTypeGooglePubSub PublisherType = "google_pubsub"
)

// Config for wal-listener.
type Config struct {
	Listener   *ListenerCfg  `valid:"required"`
	Database   *DatabaseCfg  `valid:"required" mapstructure:"database"`
	Publisher  *PublisherCfg `valid:"required"`
	Logger     *cfg.Logger   `valid:"required"`
	Monitoring cfg.Monitoring
}

// ListenerCfg path of the listener config.
type ListenerCfg struct {
	SlotName          string `valid:"required"`
	ServerPort        int
	AckTimeout        time.Duration
	RefreshConnection time.Duration `valid:"required"`
	HeartbeatInterval time.Duration `valid:"required"`
	Include           IncludeStruct
	Exclude           ExcludeStruct
	TopicsMap         map[*regexp.Regexp]string `valid:"-"`
}

// PublisherCfg represent configuration for any publisher types.
type PublisherCfg struct {
	Type            PublisherType `valid:"required"`
	Address         string
	Topic           string `valid:"required"`
	TopicPrefix     string
	EnableTLS       bool   `mapstructure:"enable_tls"`
	ClientCert      string `mapstructure:"client_cert"`
	ClientKey       string `mapstructure:"client_key"`
	CACert          string `mapstructure:"ca_cert"`
	PubSubProjectID string `mapstructure:"pubsub_project_id"`
	EnableOrdering  bool   `mapstructure:"enable_ordering"`
}

// DatabaseCfg path of the PostgreSQL DB config.
type DatabaseCfg struct {
	Host     string `valid:"required"`
	Port     uint16 `valid:"required"`
	Name     string `valid:"required"`
	User     string `valid:"required"`
	Password string `valid:"required" mapstructure:"password"`
	SSL      bool   `mapstructure:"ssl"`
}

// IncludeStruct incoming WAL message filter.
type IncludeStruct struct {
	Tables map[string][]string
}

// ExcludeStruct incoming WAL message filter.
type ExcludeStruct struct {
	Tables []string
}

// Validate config data.
func (c Config) Validate() error {
	_, err := govalidator.ValidateStruct(c)
	return err
}

// InitConfig load config from file.
func InitConfig(path string) (*Config, error) {
	var conf Config
	v := viper.NewWithOptions(viper.KeyDelimiter("::"))
	v.SetDefault("database::password", os.Getenv("DATABASE_PASSWORD"))
	v.SetConfigFile(path)
	if err := v.ReadInConfig(); err != nil {
		return nil, fmt.Errorf("error reading config: %w", err)
	}

	if err := v.Unmarshal(&conf, viper.DecodeHook(
		mapstructure.ComposeDecodeHookFunc(
			mapstructure.StringToTimeDurationHookFunc(),
			mapstructure.StringToSliceHookFunc(","),
			regexpMapHook(),
		),
	)); err != nil {
		return nil, fmt.Errorf("unable to decode into config struct: %w", err)
	}

	return &conf, nil
}

func regexpMapHook() mapstructure.DecodeHookFuncType {
	return func(
		f reflect.Type,
		t reflect.Type,
		data interface{},
	) (interface{}, error) {
		if f.Kind() != reflect.Map || f.Key().Kind() != reflect.String || f.Elem().Kind() != reflect.Interface {
			return data, nil
		}

		if t != reflect.TypeOf(map[*regexp.Regexp]string{}) {
			return data, nil
		}

		inputMap := data.(map[string]interface{})
		outputMap := make(map[*regexp.Regexp]string)

		for key, value := range inputMap {
			var re *regexp.Regexp
			var err error

			if len(key) > 1 && key[0] == '/' && key[len(key)-1] == '/' {
				re, err = regexp.Compile(key[1 : len(key)-1])
				if err != nil {
					return nil, fmt.Errorf("invalid regexp topic map regexp %s: %w", key, err)
				}
			} else {
				re, err = regexp.Compile("^" + regexp.QuoteMeta(key) + "$")
				if err != nil {
					return nil, fmt.Errorf("invalid static topic map key %s: %w", key, err)
				}
			}

			outputMap[re] = value.(string)
		}

		return outputMap, nil
	}
}
