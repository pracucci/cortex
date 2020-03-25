package storegateway

import (
	"flag"
	"fmt"
	"os"
	"time"

	"github.com/go-kit/kit/log/level"

	"github.com/cortexproject/cortex/pkg/ring"
	"github.com/cortexproject/cortex/pkg/ring/kv"
	"github.com/cortexproject/cortex/pkg/util"
	"github.com/cortexproject/cortex/pkg/util/flagext"
)

const (
	// StoreGatewayRingKey is the key under which we store the store gateways ring in the KVStore.
	RingKey = "store-gateway"

	// StoreGatewayRingName is the name of the ring used by the store gateways.
	RingName = "store-gateway"

	// We use a safe default instead of exposing to config option to the user
	// in order to simplify the config.
	RingNumTokens = 512
)

// RingConfig masks the ring lifecycler config which contains
// many options not really required by the store gateways ring. This config
// is used to strip down the config to the minimum, and avoid confusion
// to the user.
type RingConfig struct {
	KVStore           kv.Config     `yaml:"kvstore"`
	HeartbeatPeriod   time.Duration `yaml:"heartbeat_period"`
	HeartbeatTimeout  time.Duration `yaml:"heartbeat_timeout"`
	ReplicationFactor int           `yaml:"replication_factor"`

	// Instance details
	InstanceID             string   `yaml:"instance_id" doc:"hidden"`
	InstanceInterfaceNames []string `yaml:"instance_interface_names" doc:"hidden"`
	InstancePort           int      `yaml:"instance_port" doc:"hidden"`
	InstanceAddr           string   `yaml:"instance_addr" doc:"hidden"`

	// Injected internally
	ListenPort int `yaml:"-"`
}

// RegisterFlags adds the flags required to config this to the given FlagSet
func (cfg *RingConfig) RegisterFlags(f *flag.FlagSet) {
	hostname, err := os.Hostname()
	if err != nil {
		level.Error(util.Logger).Log("msg", "failed to get hostname", "err", err)
		os.Exit(1)
	}

	// Ring flags
	cfg.KVStore.RegisterFlagsWithPrefix("store-gateway.ring.", "collectors/", f)
	f.DurationVar(&cfg.HeartbeatPeriod, "store-gateway.ring.heartbeat-period", 5*time.Second, "Period at which to heartbeat to the ring.")
	f.DurationVar(&cfg.HeartbeatTimeout, "store-gateway.ring.heartbeat-timeout", time.Minute, "The heartbeat timeout after which store gateways are considered unhealthy within the ring.")
	f.IntVar(&cfg.ReplicationFactor, "store-gateway.replication-factor", 2, "The replication factor to use when sharding blocks.")

	// Instance flags
	cfg.InstanceInterfaceNames = []string{"eth0", "en0"}
	f.Var((*flagext.Strings)(&cfg.InstanceInterfaceNames), "store-gateway.ring.instance-interface", "Name of network interface to read address from.")
	f.StringVar(&cfg.InstanceAddr, "store-gateway.ring.instance-addr", "", "IP address to advertise in the ring.")
	f.IntVar(&cfg.InstancePort, "store-gateway.ring.instance-port", 0, "Port to advertise in the ring (defaults to server.grpc-listen-port).")
	f.StringVar(&cfg.InstanceID, "store-gateway.ring.instance-id", hostname, "Instance ID to register in the ring.")
}

func (cfg *RingConfig) ToRingConfig() ring.Config {
	rc := ring.Config{}
	flagext.DefaultValues(&rc)

	rc.KVStore = cfg.KVStore
	rc.HeartbeatTimeout = cfg.HeartbeatTimeout
	rc.ReplicationFactor = cfg.ReplicationFactor

	return rc
}

func (cfg *RingConfig) ToBasicLifecyclerConfig() (ring.BasicLifecyclerConfig, error) {
	instanceAddr, err := ring.GetInstanceAddr(cfg.InstanceAddr, cfg.InstanceInterfaceNames)
	if err != nil {
		return ring.BasicLifecyclerConfig{}, err
	}

	instancePort := ring.GetInstancePort(cfg.InstancePort, cfg.ListenPort)

	return ring.BasicLifecyclerConfig{
		ID:                  cfg.InstanceID,
		Addr:                fmt.Sprintf("%s:%d", instanceAddr, instancePort),
		HeartbeatPeriod:     cfg.HeartbeatPeriod,
		TokensObservePeriod: 0,
		NumTokens:           RingNumTokens,
	}, nil
}
