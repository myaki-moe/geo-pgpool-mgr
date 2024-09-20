package main

import (
	"context"
	"encoding/json"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"reflect"
	"strconv"
	"strings"
	"sync"
	"syscall"
	"time"

	clientv3 "go.etcd.io/etcd/client/v3"
	"go.uber.org/zap"
	"gopkg.in/yaml.v2"
)

type Config struct {
	Pgpool struct {
		ConfigAppend  string `yaml:"config_append"`
		TmpStatusPath string `yaml:"tmp_status_path"`
		TmpConfigPath string `yaml:"tmp_config_path"`
		ExecPath      string `yaml:"exec_path"`
		FailDelay     int    `yaml:"fail_delay"`
	} `yaml:"pgpool"`
	Etcd3 struct {
		Host      string `yaml:"host"`
		Port      int    `yaml:"port"`
		Username  string `yaml:"username"`
		Password  string `yaml:"password"`
		Scope     string `yaml:"scope"`
		Namespace string `yaml:"namespace"`
		Timeout   int    `yaml:"timeout"`
	} `yaml:"etcd3"`
	LocalBackends []struct {
		Name   string `yaml:"name"`
		Weight int    `yaml:"weight"`
	} `yaml:"local_backends"`
}

type BackendInfo struct {
	ConnURL          string `json:"conn_url"`
	APIURL           string `json:"api_url"`
	State            string `json:"state"`
	Role             string `json:"role"`
	Version          string `json:"version"`
	XlogLocation     int64  `json:"xlog_location"`
	ReplicationState string `json:"replication_state"`
	Timeline         int    `json:"timeline"`
}

type PostgresBackendStatus int

const (
	UNKNOWN PostgresBackendStatus = iota
	MASTER
	SLAVE
)

func (s PostgresBackendStatus) String() string {
	switch s {
	case MASTER:
		return "MASTER"
	case SLAVE:
		return "SLAVE"
	default:
		return "UNKNOWN"
	}
}

type PostgresBackend struct {
	Hostname string
	Port     int
	Weight   float64
	Status   PostgresBackendStatus
}

func loadConfig(filePath string) (*Config, error) {
	// Read and parse yaml file
	yamlFile, err := os.ReadFile(filePath)
	if err != nil {
		return nil, err
	}
	var config Config
	err = yaml.Unmarshal(yamlFile, &config)
	if err != nil {
		return nil, err
	}

	// Default values
	if config.Pgpool.TmpStatusPath == "" {
		config.Pgpool.TmpStatusPath = "/tmp/pgpool_status"
		logger.Debug("Using default Pgpool temporary status path: /tmp/pgpool_status")
	}
	if config.Pgpool.TmpConfigPath == "" {
		config.Pgpool.TmpConfigPath = "/tmp/pgpool.conf"
		logger.Debug("Using default Pgpool temporary config path: /tmp/pgpool.conf")
	}
	if config.Pgpool.ExecPath == "" {
		config.Pgpool.ExecPath = "pgpool"
		logger.Debug("Using default Pgpool execution path: pgpool")
	}
	if config.Etcd3.Port == 0 {
		config.Etcd3.Port = 2379
		logger.Debug("Using default Etcd3 port: 2379")
	}
	if config.Etcd3.Timeout == 0 {
		config.Etcd3.Timeout = 10
		logger.Debug("Using default Etcd3 timeout: 10 seconds")
	}

	// Check required fields
	if config.Etcd3.Host == "" {
		return nil, fmt.Errorf("missing etcd3 host config")
	}
	if len(config.LocalBackends) == 0 {
		return nil, fmt.Errorf("missing local backend config")
	}

	for _, backend := range config.LocalBackends {
		if backend.Name == "" {
			return nil, fmt.Errorf("missing backend name")
		}
	}

	return &config, nil
}

func validateBackends(backends []PostgresBackend) error {
	if len(backends) == 0 {
		return fmt.Errorf("backend list is empty")
	}

	var masterBackend *PostgresBackend
	for _, backend := range backends {
		if backend.Status == MASTER {
			if masterBackend != nil {
				return fmt.Errorf("multiple master backends found")
			}
			masterBackend = &backend
		}
	}

	if masterBackend == nil {
		return fmt.Errorf("no master backend found")
	}

	return nil
}

func generatePgpoolConfig(backends []PostgresBackend, configAppend string) (string, error) {

	// validate backends
	if err := validateBackends(backends); err != nil {
		return "", fmt.Errorf("invalid backends configuration: %w", err)
	}

	var masterBackend *PostgresBackend
	var slaveBackends []PostgresBackend
	for _, backend := range backends {
		if backend.Status == MASTER {
			masterBackend = &backend
		} else if backend.Status == SLAVE {
			slaveBackends = append(slaveBackends, backend)
		}
	}

	var config strings.Builder

	// Generate master backend config
	fmt.Fprintf(&config, "backend_hostname0 = %s\n", masterBackend.Hostname)
	fmt.Fprintf(&config, "backend_port0 = %d\n", masterBackend.Port)
	fmt.Fprintf(&config, "backend_weight0 = %.2f\n", masterBackend.Weight)
	fmt.Fprintf(&config, "backend_flag0 = 'DISALLOW_TO_FAILOVER|ALWAYS_PRIMARY'\n")
	fmt.Fprintln(&config, "")

	// Generate slave backend config
	for i, slave := range slaveBackends {
		fmt.Fprintf(&config, "backend_hostname%d = %s\n", i+1, slave.Hostname)
		fmt.Fprintf(&config, "backend_port%d = %d\n", i+1, slave.Port)
		fmt.Fprintf(&config, "backend_weight%d = %.2f\n", i+1, slave.Weight)
		fmt.Fprintf(&config, "backend_flag%d = 'DISALLOW_TO_FAILOVER'\n", i+1)
		fmt.Fprintln(&config, "")
	}

	// Append additional config
	config.WriteString(configAppend)

	logger.Debug("Generating pgpool config", zap.Int("backend_count", len(backends)))
	for i, backend := range backends {
		logger.Debug("Backend details",
			zap.Int("index", i),
			zap.String("hostname", backend.Hostname),
			zap.Int("port", backend.Port),
			zap.Float64("weight", backend.Weight),
			zap.String("status", backend.Status.String()))
	}
	logger.Debug("Generated pgpool config", zap.String("config", config.String()))

	return config.String(), nil
}

func generatePgpoolConfigFile(backends []PostgresBackend, config *Config) error {
	configStr, err := generatePgpoolConfig(backends, config.Pgpool.ConfigAppend)
	if err != nil {
		return fmt.Errorf("failed to generate configuration: %w", err)
	}

	logger.Debug("Writing tmp config file", zap.String("file_path", config.Pgpool.TmpConfigPath))

	err = os.WriteFile(config.Pgpool.TmpConfigPath, []byte(configStr), 0644)
	if err != nil {
		return fmt.Errorf("failed to write configuration file: %w", err)
	}

	return nil
}

func parseBackendInfo(jsonData []byte) (string, int, PostgresBackendStatus, error) {
	var info BackendInfo
	err := json.Unmarshal(jsonData, &info)
	if err != nil {
		return "", 0, UNKNOWN, fmt.Errorf("failed to parse backend info: %w", err)
	}

	parts := strings.Split(info.ConnURL, ":")
	if len(parts) != 3 {
		return "", 0, UNKNOWN, fmt.Errorf("invalid conn_url format")
	}
	hostname := strings.TrimPrefix(parts[1], "//")
	port := strings.Split(parts[2], "/")[0]
	portNum, err := strconv.Atoi(port)
	if err != nil {
		return "", 0, UNKNOWN, fmt.Errorf("failed to parse port: %w", err)
	}

	status := UNKNOWN
	if info.Role == "primary" {
		status = MASTER
	} else if info.Role == "replica" && (info.ReplicationState == "streaming" || info.ReplicationState == "running") {
		status = SLAVE
	}

	return hostname, portNum, status, nil
}

func getBackends(config *Config, cli *clientv3.Client) ([]PostgresBackend, error) {
	var backends []PostgresBackend

	// Get all member keys and values
	membersKey := "/" + filepath.Join(strings.Trim(config.Etcd3.Namespace, "/"), strings.Trim(config.Etcd3.Scope, "/"), "members") + "/"
	membersResp, err := cli.Get(context.Background(), membersKey, clientv3.WithPrefix())
	if err != nil {
		return nil, fmt.Errorf("failed to get member keys and values: %w", err)
	}

	// Add backends
	for _, kv := range membersResp.Kvs {
		memberName := strings.TrimPrefix(string(kv.Key), membersKey)

		hostname, port, status, err := parseBackendInfo(kv.Value)
		if err != nil {
			logger.Debug("Failed to parse backend info, ignoring", zap.String("member", memberName), zap.Error(err))
			continue
		}

		if status == UNKNOWN {
			logger.Debug("unknown backend status, ignoring", zap.String("member", memberName))
			continue
		}

		var weight float64
		var isLocal bool

		// Check if the member is a local backend
		for _, localBackend := range config.LocalBackends {
			if localBackend.Name == memberName {
				weight = float64(localBackend.Weight)
				isLocal = true
				break
			}
		}

		if status == SLAVE {
			if isLocal {
				logger.Debug("adding local slave", zap.String("name", memberName))
			} else {
				logger.Debug("skipping remote slave", zap.String("name", memberName))
				continue
			}
		} else if status == MASTER {
			if isLocal {
				logger.Debug("adding local master", zap.String("name", memberName))
			} else {
				weight = 0.0
				logger.Debug("adding remote master", zap.String("name", memberName))
			}
		}

		backends = append(backends, PostgresBackend{
			Hostname: hostname,
			Port:     port,
			Weight:   weight,
			Status:   status,
		})
		logger.Debug("backend added", zap.String("hostname", hostname), zap.Int("port", port), zap.Float64("weight", weight), zap.String("status", status.String()))
	}

	return backends, nil
}

func watchBackendChanges(config *Config, startCh chan<- bool, restartCh chan<- bool) error {
	// etcd3 client
	cli, err := clientv3.New(clientv3.Config{
		Endpoints:   []string{fmt.Sprintf("%s:%d", config.Etcd3.Host, config.Etcd3.Port)},
		DialTimeout: time.Duration(config.Etcd3.Timeout) * time.Second,
		Username:    config.Etcd3.Username,
		Password:    config.Etcd3.Password,
	})
	if err != nil {
		return fmt.Errorf("failed to create etcd client: %w", err)
	}
	defer cli.Close()

	var backends []PostgresBackend

	// Initial read of backend information
	logger.Info("reading initial backend information")
	backends, err = getBackends(config, cli)
	if err != nil {
		return fmt.Errorf("failed to get initial backend info: %w", err)
	}
	// generate initial pgpool config file
	err = generatePgpoolConfigFile(backends, config)
	if err != nil {
		return fmt.Errorf("failed to generate initial pgpool config file: %w", err)
	}

	// signal to start pgpool for the first time
	startCh <- true

	// Watch for changes in etcd
	membersKey := "/" + filepath.Join(strings.Trim(config.Etcd3.Namespace, "/"), strings.Trim(config.Etcd3.Scope, "/"), "members") + "/"
	leaderKey := "/" + filepath.Join(strings.Trim(config.Etcd3.Namespace, "/"), strings.Trim(config.Etcd3.Scope, "/"), "leader")
	watchChan := cli.Watch(context.Background(), membersKey, clientv3.WithPrefix())
	leaderWatchChan := cli.Watch(context.Background(), leaderKey)

	logger.Info("Starting to watch for backend and leader changes", zap.String("members_key", membersKey), zap.String("leader_key", leaderKey))

	for {
		select {
		case watchResp := <-watchChan:
			if watchResp.Err() != nil {
				return fmt.Errorf("member watch error: %w", watchResp.Err())
			}
			logger.Debug("detected change in backend info")
		case leaderWatchResp := <-leaderWatchChan:
			if leaderWatchResp.Err() != nil {
				return fmt.Errorf("leader watch error: %w", leaderWatchResp.Err())
			}
			logger.Debug("detected change in leader info")
		}

		newBackends, err := getBackends(config, cli)
		if err != nil {
			return fmt.Errorf("failed to get updated backend info: %w", err)
		}

		if err := validateBackends(newBackends); err != nil {
			logger.Debug("invalid backends config, maybe the cluster state is transitioning, skipping", zap.Error(err))
			continue
		}

		if !reflect.DeepEqual(newBackends, backends) {
			err = generatePgpoolConfigFile(newBackends, config)
			if err != nil {
				return fmt.Errorf("failed to generate updated pgpool config file: %w", err)
			}
			backends = newBackends
			logger.Info("updated pgpool config file", zap.Int("all_backends", len(newBackends)))

			// Signal to restart pgpool
			restartCh <- true
		}
	}
}

func runPgpool(config *Config, restartCh <-chan bool) error {
	// remove stale status file
	err := os.Remove(config.Pgpool.TmpStatusPath)
	if err != nil && !os.IsNotExist(err) {
		logger.Warn("cannot delete stale status file", zap.String("path", config.Pgpool.TmpStatusPath), zap.Error(err))
	}

	cmd := exec.Command(config.Pgpool.ExecPath, "-n", "-f", config.Pgpool.TmpConfigPath)
	cmd.Stdout = os.Stdout
	cmd.Stderr = os.Stderr

	doneCh := make(chan bool)

	go func() {
		err := cmd.Start()
		logger.Info("starting pgpool process", zap.String("pid", strconv.Itoa(cmd.Process.Pid)))
		if err != nil {
			logger.Error("failed to start pgpool", zap.Error(err))
		} else {
			cmd.Wait()
		}
		doneCh <- true
	}()

	select {
	case <-restartCh:
		if cmd.Process != nil {
			logger.Info("received restart signal, stopping pgpool", zap.String("pid", strconv.Itoa(cmd.Process.Pid)))
			err := cmd.Process.Signal(syscall.SIGTERM)
			if err != nil {
				cmd.Process.Kill()
				logger.Debug("failed to send SIGTERM to pgpool, SIGKILL sent", zap.Error(err))
			}
			cmd.Wait()
			logger.Debug("pgpool process stopped and ready to restart", zap.String("pid", strconv.Itoa(cmd.Process.Pid)))
		}
	case <-doneCh:
		logger.Debug("pgpool process stopped unexpectedly", zap.String("pid", strconv.Itoa(cmd.Process.Pid)))
		if config.Pgpool.FailDelay > 0 {
			logger.Debug("restarting pgpool after delay", zap.Int("delay", config.Pgpool.FailDelay))
			time.Sleep(time.Duration(config.Pgpool.FailDelay) * time.Second)
		}
	}

	return nil
}

var logger *zap.Logger

func init() {
	var err error
	logger, err = zap.NewProduction()
	if err != nil {
		panic("cannot initialize logger: " + err.Error())
	}
}

func main() {
	defer logger.Sync()

	logger.Info("pgpool-mgr v0.0.1 started", zap.Strings("args", os.Args[1:]))

	// parse command line arguments
	configFile := ""
	verboseOutput := false
	for _, arg := range os.Args[1:] {
		if arg == "-v" {
			verboseOutput = true
		} else {
			configFile = arg
		}
	}
	if verboseOutput {
		config := zap.NewProductionConfig()
		config.Level = zap.NewAtomicLevelAt(zap.DebugLevel)
		logger, _ = config.Build()
		logger.Debug("verbose output enabled")
	}
	if configFile == "" {
		configFile = "/etc/geo-pgpool-mgr/config.yaml"
		logger.Warn("using default config file", zap.String("file", configFile))
	}

	// load configuration
	config, err := loadConfig(configFile)
	if err != nil {
		logger.Fatal("failed to load configuration", zap.Error(err))
	}
	logger.Debug("loaded configuration", zap.Any("config", config))

	// print nodes and weights
	logger.Info("loaded node and weight:")
	for _, backend := range config.LocalBackends {
		logger.Info("backend node",
			zap.String("name", backend.Name),
			zap.Int("weight", backend.Weight))
	}

	startCh := make(chan bool)
	restartCh := make(chan bool)

	var wg sync.WaitGroup
	wg.Add(2)

	go func() {
		defer wg.Done()
		for {
			err := watchBackendChanges(config, startCh, restartCh)
			if err != nil {
				logger.Error("failed to watch backend changes", zap.Error(err))
			}
			logger.Debug("watchBackendChanges exited, restarting...")
		}
	}()

	go func() {
		defer wg.Done()
		// waiting for pgpool config file ready
		<-startCh

		for {
			err = runPgpool(config, restartCh)
			if err != nil {
				logger.Error("failed to run pgpool", zap.Error(err))
			}
			logger.Debug("runPgpool exited, restarting...")
		}
	}()

	wg.Wait()

	logger.Info("exiting")
	os.Exit(0)
}
