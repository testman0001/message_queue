package main

import (
	"crypto/tls"
	"flag"
	"fmt"
	"log"
	"math/rand"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"syscall"
	"time"

	"github.com/BurntSushi/toml"
	"github.com/absolute8511/glog"
	"github.com/judwhite/go-svc/svc"
	"github.com/mreiferson/go-options"
	"github.com/youzan/nsq/consistence"
	"github.com/youzan/nsq/internal/app"
	"github.com/youzan/nsq/internal/version"
	"github.com/youzan/nsq/nsqd"
	"github.com/youzan/nsq/nsqdserver"
)

type tlsRequiredOption int

func (t *tlsRequiredOption) Set(s string) error {
	s = strings.ToLower(s)
	if s == "tcp-https" {
		*t = nsqd.TLSRequiredExceptHTTP
		return nil
	}
	required, err := strconv.ParseBool(s)
	if required {
		*t = nsqd.TLSRequired
	} else {
		*t = nsqd.TLSNotRequired
	}
	return err
}

func (t *tlsRequiredOption) Get() interface{} { return int(*t) }

func (t *tlsRequiredOption) String() string {
	return strconv.FormatInt(int64(*t), 10)
}

func (t *tlsRequiredOption) IsBoolFlag() bool { return true }

type tlsMinVersionOption uint16

func (t *tlsMinVersionOption) Set(s string) error {
	s = strings.ToLower(s)
	switch s {
	case "":
		return nil
	case "ssl3.0":
		*t = tls.VersionSSL30
	case "tls1.0":
		*t = tls.VersionTLS10
	case "tls1.1":
		*t = tls.VersionTLS11
	case "tls1.2":
		*t = tls.VersionTLS12
	default:
		return fmt.Errorf("unknown tlsVersionOption %q", s)
	}
	return nil
}

func (t *tlsMinVersionOption) Get() interface{} { return uint16(*t) }

func (t *tlsMinVersionOption) String() string {
	return strconv.FormatInt(int64(*t), 10)
}

func nsqdFlagSet(opts *nsqd.Options) *flag.FlagSet {
	flagSet := flag.NewFlagSet("nsqd", flag.ExitOnError)

	// basic options
	flagSet.Bool("version", false, "print version string")
	flagSet.Bool("verbose", false, "enable verbose logging")
	flagSet.String("config", "", "path to config file")
	flagSet.Int64("worker-id", opts.ID, "unique seed for message ID generation (int) in range [0,4096) (will default to a hash of hostname)")

	flagSet.String("cluster-id", opts.ClusterID, "cluster id for nsq")
	flagSet.String("cluster-leadership-addresses", opts.ClusterLeadershipAddresses, "cluster leadership server list for nsq")
	flagSet.String("cluster-leadership-username", opts.ClusterLeadershipUsername, "cluster leadership server username for nsq")
	flagSet.String("cluster-leadership-password", opts.ClusterLeadershipPassword, "cluster leadership server password for nsq")
	flagSet.String("cluster-leadership-root-dir", opts.ClusterLeadershipRootDir, "cluster leadership server root dir for nsq")

	flagSet.String("https-address", opts.HTTPSAddress, "<addr>:<port> to listen on for HTTPS clients")
	flagSet.String("http-address", opts.HTTPAddress, "<addr>:<port> to listen on for HTTP clients")
	flagSet.String("metric-address", opts.MetricAddress, "<addr>:<port> to listen on for HTTP metric clients")
	flagSet.String("tcp-address", opts.TCPAddress, "<addr>:<port> to listen on for TCP clients")
	flagSet.String("rpc-port", opts.RPCPort, "<port> to listen on for RPC communication")
	flagSet.String("reverse-proxy-port", opts.ReverseProxyPort, "<port> for reverse proxy port")
	authHTTPAddresses := app.StringArray{}
	flagSet.Var(&authHTTPAddresses, "auth-http-address", "<addr>:<port> to query auth server (may be given multiple times)")
	flagSet.String("broadcast-address", opts.BroadcastAddress, "address that will be registered with lookupd (defaults to the OS hostname)")
	flagSet.String("broadcast-interface", opts.BroadcastInterface, "address that will be registered with lookupd (defaults to the OS hostname)")
	lookupdTCPAddrs := app.StringArray{}
	flagSet.Var(&lookupdTCPAddrs, "lookupd-tcp-address", "lookupd TCP address (may be given multiple times)")
	flagSet.String("lookup-ping-interval", opts.LookupPingInterval.String(), "duration between ping to nsqlookup")

	// diskqueue options
	flagSet.String("data-path", opts.DataPath, "path to store disk-backed messages")
	flagSet.Int64("mem-queue-size", opts.MemQueueSize, "number of messages to keep in memory (per topic/channel)")
	flagSet.Int64("max-bytes-per-file", opts.MaxBytesPerFile, "number of bytes per diskqueue file before rolling")
	flagSet.Int64("sync-every", opts.SyncEvery, "number of messages per diskqueue fsync")
	flagSet.Duration("sync-timeout", opts.SyncTimeout, "duration of time per diskqueue fsync")

	// msg and command options
	flagSet.String("msg-timeout", opts.MsgTimeout.String(), "duration to wait before auto-requeing a message")
	flagSet.Duration("max-msg-timeout", opts.MaxMsgTimeout, "maximum duration before a message will timeout")
	flagSet.Int64("max-msg-size", opts.MaxMsgSize, "maximum size of a single message in bytes")
	flagSet.Duration("max-req-timeout", opts.MaxReqTimeout, "maximum requeuing timeout for a message")
	flagSet.Duration("req-to-end-threshold", opts.ReqToEndThreshold, "duration threshold for requeue message to queue end")
	// remove, deprecated
	flagSet.Int64("max-message-size", opts.MaxMsgSize, "(deprecated use --max-msg-size) maximum size of a single message in bytes")
	flagSet.Int64("max-body-size", opts.MaxBodySize, "maximum size of a single command body")
	flagSet.Int64("max-pub-waiting-size", opts.MaxPubWaitingSize, "maximum size of a topic partition waiting pub bytes")
	flagSet.Duration("ack-old-than-time", opts.AckOldThanTime, "max duration before  auto ack is considered too old")
	flagSet.Int("ack-retry-cnt", opts.AckRetryCnt, "max retry count for auto ack, default 0 will never auto ack")

	// client overridable configuration options
	flagSet.Duration("max-heartbeat-interval", opts.MaxHeartbeatInterval, "maximum client configurable duration of time between client heartbeats")
	flagSet.Int64("max-rdy-count", opts.MaxRdyCount, "maximum RDY count for a client")
	flagSet.Int64("max-output-buffer-size", opts.MaxOutputBufferSize, "maximum client configurable size (in bytes) for a client output buffer")
	flagSet.Duration("max-output-buffer-timeout", opts.MaxOutputBufferTimeout, "maximum client configurable duration of time between flushing to a client")
	flagSet.Int64("max-confirm-win", opts.MaxConfirmWin, "maximum confirm window (in bytes)")
	flagSet.Int64("max-channel-delayed-qnum", opts.MaxChannelDelayedQNum, "maximum messages in delayed queue for each channel")
	flagSet.Int64("channel-ratelimit-kb", opts.ChannelRateLimitKB, "the default rate limit kilobytes for each channel")

	// statsd integration options
	flagSet.String("statsd-address", opts.StatsdAddress, " <addr>:<port> of a statsd daemon for pushing stats")
	flagSet.String("statsd-protocol", opts.StatsdProtocol, "protocol of a statsd daemon for pushing stats")
	flagSet.String("statsd-interval", opts.StatsdInterval.String(), "duration between pushing to statsd")
	flagSet.Bool("statsd-mem-stats", opts.StatsdMemStats, "toggle sending memory and GC stats to statsd")
	flagSet.String("statsd-prefix", opts.StatsdPrefix, "prefix used for keys sent to statsd (%s for host replacement)")

	// End to end percentile flags
	e2eProcessingLatencyPercentiles := app.FloatArray{}
	flagSet.Var(&e2eProcessingLatencyPercentiles, "e2e-processing-latency-percentile", "message processing time percentiles (as float (0, 1.0]) to track (can be specified multiple times or comma separated '1.0,0.99,0.95', default none)")
	flagSet.Duration("e2e-processing-latency-window-time", opts.E2EProcessingLatencyWindowTime, "calculate end to end latency quantiles for this duration of time (ie: 60s would only show quantile calculations from the past 60 seconds)")

	// TLS config
	flagSet.String("tls-cert", opts.TLSCert, "path to certificate file")
	flagSet.String("tls-key", opts.TLSKey, "path to key file")
	flagSet.String("tls-client-auth-policy", opts.TLSClientAuthPolicy, "client certificate auth policy ('require' or 'require-verify')")
	flagSet.String("tls-root-ca-file", opts.TLSRootCAFile, "path to certificate authority file")
	tlsRequired := tlsRequiredOption(opts.TLSRequired)
	tlsMinVersion := tlsMinVersionOption(opts.TLSMinVersion)
	flagSet.Var(&tlsRequired, "tls-required", "require TLS for client connections (true, false, tcp-https)")
	flagSet.Var(&tlsMinVersion, "tls-min-version", "minimum SSL/TLS version acceptable ('ssl3.0', 'tls1.0', 'tls1.1', or 'tls1.2')")

	// compression
	flagSet.Bool("deflate", opts.DeflateEnabled, "enable deflate feature negotiation (client compression)")
	flagSet.Int("max-deflate-level", opts.MaxDeflateLevel, "max deflate compression level a client can negotiate (> values == > nsqd CPU usage)")
	flagSet.Bool("snappy", opts.SnappyEnabled, "enable snappy feature negotiation (client compression)")
	flagSet.Int("log-level", int(opts.LogLevel), "log verbose level")
	flagSet.String("log-dir", opts.LogDir, "directory for logs")
	flagSet.String("remote-tracer", opts.RemoteTracer, "server for message tracing")
	flagSet.Int("retention-days", int(opts.RetentionDays), "the default retention days for topic data")
	flagSet.Int64("retention-size-per-day", int64(opts.RetentionSizePerDay), "the default retention bytes in a day for topic data")
	flagSet.Bool("start-as-fix-mode", opts.StartAsFixMode, "enable data fix at start")
	flagSet.Bool("allow-ext-compatible", opts.AllowExtCompatible, "allow pub ext to non-ext topic(ignore ext) .")
	flagSet.Bool("allow-sub-ext-compatible", opts.AllowSubExtCompatible, "allow sub ext-topic without ext in message.")

	flagSet.Duration("queue-scan-interval", opts.QueueScanInterval, "scan interval")
	flagSet.Duration("queue-scan-refresh-interval", opts.QueueScanRefreshInterval, "scan refresh interval for new channels")
	flagSet.Int("queue-scan-selection-count", opts.QueueScanSelectionCount, "select count for each scan")
	flagSet.Int("queue-scan-worker-pool-max", opts.QueueScanWorkerPoolMax, "the max scan worker pool")
	flagSet.Int("queue-topic-job-worker-pool-max", opts.QueueTopicJobWorkerPoolMax, "the max scan worker pool")
	flagSet.Float64("queue-scan-dirty-percent", opts.QueueScanDirtyPercent, "retry scan immediately if dirty percent happened in last scan")
	flagSet.Bool("allow-zan-test-skip", opts.AllowZanTestSkip, "allow zan test message filter in new created channel & channels under newly upgraded topic")
	flagSet.Int("default-commit-buf", int(opts.DefaultCommitBuf), "the default commit buffer for topic data")
	flagSet.Int("max-commit-buf", int(opts.MaxCommitBuf), "the max commit buffer for topic data")
	flagSet.Bool("use-fsync", opts.UseFsync, "use fsync while flush data")
	flagSet.Int("max-conn-for-client", int(opts.MaxConnForClient), "the max connections for all clients")
	flagSet.Int("queue-read-buffer-size", int(opts.QueueReadBufferSize), "the read buffer size for topic disk queue file")
	flagSet.Int("queue-write-buffer-size", int(opts.QueueWriteBufferSize), "the write buffer size for topic disk queue file")
	flagSet.Int("pub-queue-size", int(opts.PubQueueSize), "the pub queue size for topic")
	flagSet.Int("sleepms-between-log-sync-pull", int(opts.SleepMsBetweenLogSyncPull), "the sleep ms between each log sync pull")

	flagSet.Bool("kv-enabled", opts.KVEnabled, "enable the kv topic")
	flagSet.Int("kv-block-cache", int(opts.KVBlockCache), "kv engine block cache")
	flagSet.Int("kv-write-buffer-size", int(opts.KVWriteBufferSize), "kv engine write buffer size")
	flagSet.Int("kv-max-write-buffer-number", int(opts.KVMaxWriteBufferNumber), "kv max write buffer number")
	return flagSet
}

type config map[string]interface{}

// Validate settings in the config file, and fatal on errors
func (cfg config) Validate() {
	// special validation/translation
	if v, exists := cfg["tls_required"]; exists {
		var t tlsRequiredOption
		err := t.Set(fmt.Sprintf("%v", v))
		if err == nil {
			cfg["tls_required"] = t.String()
		} else {
			log.Fatalf("ERROR: failed parsing tls required %v", v)
		}
	}
	if v, exists := cfg["tls_min_version"]; exists {
		var t tlsMinVersionOption
		err := t.Set(fmt.Sprintf("%v", v))
		if err == nil {
			newVal := fmt.Sprintf("%v", t.Get())
			if newVal != "0" {
				cfg["tls_min_version"] = newVal
			} else {
				delete(cfg, "tls_min_version")
			}
		} else {
			log.Fatalf("ERROR: failed parsing tls min version %v", v)
		}
	}
}

type program struct {
	nsqdServer *nsqdserver.NsqdServer
}

func main() {
	defer glog.Flush()
	prg := &program{}
	if err := svc.Run(prg, os.Interrupt, syscall.SIGTERM, syscall.SIGHUP, syscall.SIGQUIT, syscall.SIGINT); err != nil {
		log.Panic(err)
	}
	log.Println("app exited.")
}

func (p *program) Init(env svc.Environment) error {
	if env.IsWindowsService() {
		dir := filepath.Dir(os.Args[0])
		return os.Chdir(dir)
	}
	return nil
}

func (p *program) Start() error {
	opts := nsqd.NewOptions()

	flagSet := nsqdFlagSet(opts)
	glog.InitWithFlag(flagSet)

	flagSet.Parse(os.Args[1:])

	rand.Seed(time.Now().UTC().UnixNano())

	if flagSet.Lookup("version").Value.(flag.Getter).Get().(bool) {
		fmt.Println(version.String("nsqd"))
		os.Exit(0)
	}

	var cfg config
	configFile := flagSet.Lookup("config").Value.String()
	if configFile != "" {
		_, err := toml.DecodeFile(configFile, &cfg)
		if err != nil {
			log.Fatalf("ERROR: failed to load config file %s - %s", configFile, err.Error())
		}
	}
	cfg.Validate()

	options.Resolve(opts, flagSet, cfg)
	if opts.LogDir != "" {
		glog.SetGLogDir(opts.LogDir)
	}
	glog.SetLogExitFunc(func(err error) {
		fmt.Printf("glog error: %v\n", err)
	})
	glog.StartWorker(time.Second * 2)

	if strings.TrimSpace(opts.ClusterLeadershipRootDir) != "" {
		consistence.NSQ_ROOT_DIR = opts.ClusterLeadershipRootDir
	}

	// if we are using the coordinator, we should disable the topic at startup
	initDisabled := int32(0)
	if opts.RPCPort != "" {
		initDisabled = 1
	}
	nsqd.SetLogger(opts.Logger)
	nsqd.SetRemoteMsgTracer(opts.RemoteTracer)

	nsqd, nsqdServer, err := nsqdserver.NewNsqdServer(opts)
	if err != nil {
		return err
	}

	nsqd.LoadMetadata(initDisabled)
	nsqd.LoadLocalCacheTopic()
	nsqd.NotifyPersistMetadata()

	err = nsqdServer.Main()
	if err != nil {
		return err
	}
	p.nsqdServer = nsqdServer
	return nil
}

func (p *program) Stop() error {
	if p.nsqdServer != nil {
		p.nsqdServer.Exit()
	}
	return nil
}
