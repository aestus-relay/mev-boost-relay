package cmd

import (
	"net/url"
	"os"
	"os/signal"
	"strings"
	"syscall"

	"github.com/ethereum/go-ethereum/common/hexutil"
	"github.com/flashbots/go-boost-utils/bls"
	"github.com/flashbots/mev-boost-relay/beaconclient"
	"github.com/flashbots/mev-boost-relay/common"
	"github.com/flashbots/mev-boost-relay/database"
	"github.com/flashbots/mev-boost-relay/datastore"
	"github.com/flashbots/mev-boost-relay/services/api"
	"github.com/sirupsen/logrus"
	"github.com/spf13/cobra"
)

var (
	apiDefaultListenAddr = common.GetEnv("LISTEN_ADDR", "localhost:9062")
	apiDefaultBlockSim   = common.GetEnv("BLOCKSIM_URI", "http://localhost:8545")
	apiDefaultSecretKey  = common.GetEnv("SECRET_KEY", "")
	apiDefaultLogTag     = os.Getenv("LOG_TAG")

	apiDefaultAPIs               = strings.Split(common.GetEnv("APIS", "proposer,builder,data"), ",")
	apiDefaultPprofListenAddr    = os.Getenv("PPROF_ADDR")
	apiDefaultInternalAPIEnabled = os.Getenv("ENABLE_INTERNAL_API") == "1"

	// Default Builder, Data, and Proposer API as false.
	apiDefaultBuilderAPIEnabled  = os.Getenv("ENABLE_BUILDER_API") == "1"
	apiDefaultDataAPIEnabled     = os.Getenv("ENABLE_DATA_API") == "1"
	apiDefaultProposerAPIEnabled = os.Getenv("ENABLE_PROPOSER_API") == "1"
	apiDefaultMevCommitFilteringEnabled = os.Getenv("DISABLE_MEV_COMMIT_FILTERING") != "1"

	apiMevCommitFiltering bool
	apiListenAddr         string
	apiPprofListenAddr    string
	apiSecretKey          string
	apiBlockSimURL        string
	apiDebug              bool
	apiBuilderAPI         bool
	apiDataAPI            bool
	apiInternalAPI        bool
	apiProposerAPI        bool
	apiLogTag             string
)

func init() {
	rootCmd.AddCommand(apiCmd)
	apiCmd.Flags().BoolVar(&logJSON, "json", defaultLogJSON, "log in JSON format instead of text")
	apiCmd.Flags().StringVar(&logLevel, "loglevel", defaultLogLevel, "log-level: trace, debug, info, warn/warning, error, fatal, panic")
	apiCmd.Flags().StringVar(&apiLogTag, "log-tag", apiDefaultLogTag, "if set, a 'tag' field will be added to all log entries")
	apiCmd.Flags().BoolVar(&apiDebug, "debug", false, "debug logging")

	apiCmd.Flags().StringVar(&apiListenAddr, "listen-addr", apiDefaultListenAddr, "listen address for webserver")
	apiCmd.Flags().StringSliceVar(&beaconNodeURIs, "beacon-uris", defaultBeaconURIs, "beacon endpoints")
	apiCmd.Flags().StringSliceVar(&beaconNodePublishURIs, "beacon-publish-uris", defaultBeaconPublishURIs, "beacon publish endpoints")
	apiCmd.Flags().StringVar(&redisURI, "redis-uri", defaultRedisURI, "redis uri")
	apiCmd.Flags().StringVar(&redisReadonlyURI, "redis-readonly-uri", defaultRedisReadonlyURI, "redis readonly uri")
	apiCmd.Flags().StringVar(&bidEngineURI, "bidengine-uri", defaultBidEngineURI, "bid engine (redis) uri")
	apiCmd.Flags().StringVar(&bidEngineROURI, "bidengine-ro-uri", defaultBidEngineURI, "bid engine readonly (redis) uri")
	apiCmd.Flags().StringVar(&postgresDSN, "db", defaultPostgresDSN, "PostgreSQL DSN")
	apiCmd.Flags().StringSliceVar(&memcachedURIs, "memcached-uris", defaultMemcachedURIs,
		"Enable memcached, typically used as secondary backup to Redis for redundancy")
	apiCmd.Flags().StringVar(&latencySvcURI, "latency-svc-uri", defaultLatencySvcURI, "latency service uri")
	apiCmd.Flags().StringVar(&apiSecretKey, "secret-key", apiDefaultSecretKey, "secret key for signing bids")
	apiCmd.Flags().StringVar(&apiBlockSimURL, "blocksim", apiDefaultBlockSim, "URL for block simulator")
	apiCmd.Flags().StringVar(&network, "network", defaultNetwork, "Which network to use")

	apiCmd.Flags().StringSliceVar(&enabledAPIs, "apis", apiDefaultAPIs, "APIs to enable")
	apiCmd.Flags().StringVar(&apiPprofListenAddr, "pprof-listen-addr", apiDefaultPprofListenAddr, "listen address for pprof, empty to disable")
	apiCmd.Flags().BoolVar(&apiBuilderAPI, "builder-api", apiDefaultBuilderAPIEnabled, "enable builder API (/builder/...)")
	apiCmd.Flags().BoolVar(&apiDataAPI, "data-api", apiDefaultDataAPIEnabled, "enable data API (/data/...)")
	apiCmd.Flags().BoolVar(&apiInternalAPI, "internal-api", apiDefaultInternalAPIEnabled, "enable internal API (/internal/...)")
	apiCmd.Flags().BoolVar(&apiProposerAPI, "proposer-api", apiDefaultProposerAPIEnabled, "enable proposer API (/proposer/...)")
	apiCmd.Flags().BoolVar(&apiMevCommitFiltering, "mev-commit-filtering", apiDefaultMevCommitFilteringEnabled, "enable mev-commit-filtering")
}

var apiCmd = &cobra.Command{
	Use:   "api",
	Short: "Start the API server",
	Run: func(cmd *cobra.Command, args []string) {
		var err error

		if apiDebug {
			logLevel = "debug"
		}

		log := common.LogSetup(logJSON, logLevel).WithFields(logrus.Fields{
			"service": "relay/api",
			"version": Version,
		})
		if apiLogTag != "" {
			log = log.WithField("tag", apiLogTag)
		}
		log.Infof("boost-relay %s", Version)

		networkInfo, err := common.NewEthNetworkDetails(network)
		if err != nil {
			log.WithError(err).Fatalf("error getting network details")
		}
		log.Infof("Using network: %s", networkInfo.Name)
		log.Debug(networkInfo.String())

		// Connect to beacon clients and ensure it's synced
		if len(beaconNodeURIs) == 0 {
			log.Fatalf("no beacon endpoints specified")
		}
		log.Infof("Using beacon endpoints: %s", strings.Join(beaconNodeURIs, ", "))
		if len(beaconNodePublishURIs) == 0 {
			// default to same endpoint as the beacon endpoints
			beaconNodePublishURIs = beaconNodeURIs
		} else if len(beaconNodePublishURIs) != len(beaconNodeURIs) {
			log.Fatalf("beacon publish endpoints do not match the number of beacon endpoints")
		} else {
			log.Infof("Using beacon publish endpoints: %s", strings.Join(beaconNodePublishURIs, ", "))
		}

		var beaconInstances []beaconclient.IBeaconInstance
		for i, uri := range beaconNodeURIs {
			beaconInstances = append(beaconInstances, beaconclient.NewProdBeaconInstance(log, uri, beaconNodePublishURIs[i]))
		}
		beaconClient := beaconclient.NewMultiBeaconClient(log, beaconInstances)

		// Connect to Redis
		if redisReadonlyURI == "" {
			log.Infof("Connecting to Redis at %s ...", redisURI)
		} else {
			log.Infof("Connecting to Redis at %s / readonly: %s / bidengine %s / bidengine_ro %s ...", redisURI, redisReadonlyURI, bidEngineURI, bidEngineROURI)
		}
		redis, err := datastore.NewRedisCache(networkInfo.Name, redisURI, redisReadonlyURI, bidEngineURI, bidEngineROURI)
		if err != nil {
			log.WithError(err).Fatalf("Failed to connect to Redis at %s", redisURI)
		}

		// Connect to Memcached if it exists
		var mem *datastore.Memcached
		if len(memcachedURIs) > 0 {
			log.Infof("Connecting to Memcached at %s ...", strings.Join(memcachedURIs, ", "))
			mem, err = datastore.NewMemcached(networkInfo.Name, memcachedURIs...)
			if err != nil {
				log.WithError(err).Fatalf("Failed to connect to Memcached")
			}
		}

		// Connect to Postgres
		dbURL, err := url.Parse(postgresDSN)
		if err != nil {
			log.WithError(err).Fatalf("couldn't read db URL")
		}
		log.Infof("Connecting to Postgres database at %s%s ...", dbURL.Host, dbURL.Path)
		db, err := database.NewDatabaseService(postgresDSN)
		if err != nil {
			log.WithError(err).Fatalf("Failed to connect to Postgres database at %s%s", dbURL.Host, dbURL.Path)
		}

		log.Info("Setting up datastore...")
		ds, err := datastore.NewDatastore(redis, mem, db)
		if err != nil {
			log.WithError(err).Fatalf("Failed setting up prod datastore")
		}

		// Set up latency estimator
		le := api.NewLatencyEstimator(latencySvcURI, log)

		opts := api.RelayAPIOpts{
			Log:           log,
			ListenAddr:    apiListenAddr,
			BeaconClient:  beaconClient,
			Datastore:     ds,
			Redis:         redis,
			Memcached:     mem,
			DB:            db,
			LatencySvc:    le,
			EthNetDetails: *networkInfo,
			BlockSimURL:   apiBlockSimURL,

			MevCommitFiltering: apiMevCommitFiltering,
			PprofListenAddr: apiPprofListenAddr,

			BlockBuilderAPI: apiBuilderAPI,
			DataAPI:         apiDataAPI,
			InternalAPI:     apiInternalAPI,
			ProposerAPI:     apiProposerAPI,
		}

		// Update APIs from enabled list
		for _, name := range enabledAPIs {
			switch name {
			case "proposer":
				opts.ProposerAPI = true
			case "builder":
				opts.BlockBuilderAPI = true
			case "data":
				opts.DataAPI = true
			case "internal":
				opts.InternalAPI = true
			default:
				log.Fatalf("API " + name + " not recognized")
			}
		}

		// Decode the private key
		if apiSecretKey == "" {
			log.Warn("No secret key specified, block builder API is disabled")
			opts.BlockBuilderAPI = false
		} else {
			envSkBytes, err := hexutil.Decode(apiSecretKey)
			if err != nil {
				log.WithError(err).Fatal("incorrect secret key provided")
			}
			opts.SecretKey, err = bls.SecretKeyFromBytes(envSkBytes[:])
			if err != nil {
				log.WithError(err).Fatal("incorrect builder API secret key provided")
			}
		}

		// Create the relay service
		log.Info("Setting up relay service...")
		srv, err := api.NewRelayAPI(opts)
		if err != nil {
			log.WithError(err).Fatal("failed to create service")
		}

		// Create a signal handler
		sigs := make(chan os.Signal, 1)
		signal.Notify(sigs, syscall.SIGINT, syscall.SIGTERM)
		go func() {
			sig := <-sigs
			log.Infof("signal received: %s", sig)
			err := srv.StopServer()
			if err != nil {
				log.WithError(err).Fatal("error stopping server")
			}
		}()

		// Start the server
		log.Infof("Webserver starting on %s ...", apiListenAddr)
		err = srv.StartServer()
		if err != nil {
			log.WithError(err).Fatal("server error")
		}
		log.Info("bye")
	},
}
