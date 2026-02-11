package config

import (
	"os"
	"time"

	"gopkg.in/yaml.v3"
)

// Config is the root configuration.
type Config struct {
	Server      ServerConfig      `yaml:"server"`
	Persistence PersistenceConfig `yaml:"persistence"`
	Log         LogConfig         `yaml:"log"`
	AckTimeout  time.Duration     `yaml:"ack_timeout"`
}

// ServerConfig holds the configuration for all protocol servers.
type ServerConfig struct {
	GRPC GRPCConfig `yaml:"grpc"`
	HTTP HTTPConfig `yaml:"http"`
	TCP  TCPConfig  `yaml:"tcp"`
}

type GRPCConfig struct {
	Enabled bool `yaml:"enabled"`
	Port    int  `yaml:"port"`
}

type HTTPConfig struct {
	Enabled bool `yaml:"enabled"`
	Port    int  `yaml:"port"`
}

type TCPConfig struct {
	Enabled bool `yaml:"enabled"`
	Port    int  `yaml:"port"`
}

// PersistenceConfig holds AOF and snapshot settings.
type PersistenceConfig struct {
	Enabled  bool           `yaml:"enabled"`
	AOF      AOFConfig      `yaml:"aof"`
	Snapshot SnapshotConfig `yaml:"snapshot"`
}

type AOFConfig struct {
	Fsync             string `yaml:"fsync"`              // always | everysec | no
	Dir               string `yaml:"dir"`                // data directory
	Filename          string `yaml:"filename"`            // AOF file name
	RewriteMinSize    int64  `yaml:"rewrite_min_size"`    // minimum AOF size to trigger rewrite (bytes)
	RewritePercentage int    `yaml:"rewrite_percentage"`  // growth percentage to trigger rewrite
}

type SnapshotConfig struct {
	Enabled  bool          `yaml:"enabled"`
	Interval time.Duration `yaml:"interval"`
	Filename string        `yaml:"filename"`
}

type LogConfig struct {
	Level  string `yaml:"level"`  // debug | info | warn | error
	Format string `yaml:"format"` // json | console
}

// DefaultConfig returns the default configuration.
func DefaultConfig() *Config {
	return &Config{
		Server: ServerConfig{
			GRPC: GRPCConfig{Enabled: true, Port: 9090},
			HTTP: HTTPConfig{Enabled: true, Port: 8080},
			TCP:  TCPConfig{Enabled: true, Port: 7070},
		},
		Persistence: PersistenceConfig{
			Enabled: true,
			AOF: AOFConfig{
				Fsync:             "everysec",
				Dir:               "./data",
				Filename:          "mqlite.aof",
				RewriteMinSize:    67108864, // 64MB
				RewritePercentage: 100,
			},
			Snapshot: SnapshotConfig{
				Enabled:  false,
				Interval: 5 * time.Minute,
				Filename: "mqlite.snapshot",
			},
		},
		Log: LogConfig{
			Level:  "info",
			Format: "console",
		},
		AckTimeout: 30 * time.Second,
	}
}

// Load reads configuration from a YAML file, falling back to defaults.
func Load(path string) (*Config, error) {
	cfg := DefaultConfig()

	data, err := os.ReadFile(path)
	if err != nil {
		if os.IsNotExist(err) {
			return cfg, nil
		}
		return nil, err
	}

	if err := yaml.Unmarshal(data, cfg); err != nil {
		return nil, err
	}

	return cfg, nil
}
