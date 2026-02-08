# geo-pgpool-mgr

geo-pgpool-mgr is a dynamic configuration manager for Pgpool-II, designed to work with patroni and etcd for backend discovery and automatic configuration updates in geographically distributed PostgreSQL clusters.

## Introduction

While Patroni effectively implements high availability for PostgreSQL with a single primary and multiple replicas, latency becomes unavoidable in globally distributed clusters. geo-pgpool-mgr bridges this gap by combining Patroni and Pgpool-II capabilities. It automates the process of managing Pgpool-II configurations in dynamic, geographically distributed PostgreSQL cluster environments.

By reading backend server information from etcd and updating Pgpool-II configurations accordingly, geo-pgpool-mgr ensures that write requests are routed to the primary node while read requests are directed to local slave nodes, minimizing latency in global deployments.

Key features:
- Integration with Patroni and etcd for backend discovery
- Dynamic Pgpool-II configuration generation for geographically distributed clusters
- Intelligent request routing to reduce latency (writes to primary, reads to local slaves)
- Automatic Pgpool-II restarts on configuration changes
- Support for local and remote PostgreSQL backends

## How it works

In a globally distributed PostgreSQL cluster managed by Patroni, geo-pgpool-mgr operates as follows:

1. Reads the initial backend information from etcd, which is maintained by Patroni
2. Generates a Pgpool-II configuration file that:
   - Routes write requests to the current primary node
   - Routes read requests to local slave nodes when possible
3. Starts Pgpool-II with the generated configuration
4. Continuously watches for changes in etcd (backend additions, removals, role changes, or location updates)
5. Updates the Pgpool-II configuration and restarts Pgpool-II when changes occur

This process ensures that your Pgpool-II instance always has an up-to-date configuration reflecting the current state and geographical distribution of your PostgreSQL cluster. By intelligently routing requests, geo-pgpool-mgr helps minimize latency in global deployments while maintaining the high availability provided by Patroni.

## Quick Start

1. Install geo-pgpool-mgr:
   ```
   git clone https://github.com/myaki-moe/geo-pgpool-mgr.git
   cd geo-pgpool-mgr
   go build -o geo-pgpool-mgr
   ```

2. Create a configuration file (e.g., `config.yaml`):
   ```yaml
    pgpool:
      config_append: |+
        listen_addresses = '*'
        port = 5432
        backend_clustering_mode = 'streaming_replication'
        master_slave_mode = on
        master_slave_sub_mode = 'stream'
        replication_mode = false
        enable_pool_hba = off
        failover_on_backend_error = off
        load_balance_mode = true

        health_check_period = 0
        sr_check_period = 0

      tmp_status_path: /tmp/pgpool_status
      tmp_config_path: /tmp/pgpool.conf
      exec_path: /usr/bin/pgpool
      fail_delay: 5

    etcd3:
      host: 
      port: 
      username: 
      password: 
      scope: pgcluster
      namespace: /patroni/
      timeout: 10
      tls:
        # If set, TLS is enabled and this CA is used to verify the etcd server certificate
        ca: /etc/ssl/etcd/ca.pem
        # If both are set, mTLS is enabled (client certificate auth)
        client_cert: /etc/ssl/etcd/client.pem
        client_key: /etc/ssl/etcd/client-key.pem

    local_backends:
    - name: node-1
      weight: 1
    - name: node-2
      weight: 1
   ```

3. Run geo-pgpool-mgr:
   ```
   ./geo-pgpool-mgr /path/to/config.yaml
   ```

## Usage
geo-pgpool-mgr [-v] [config_file_path]

- `-v`: Enable verbose logging
- `config_file_path`: Path to the configuration file (default: `/etc/geo-pgpool-mgr/config.yaml`)

## License

This project is licensed under the GPLv3 License - see the [LICENSE](LICENSE) file for details.
