package cmd

import (
	"os"

	"github.com/flashbots/mev-boost-relay/common"
)

var (
	defaultNetwork           = common.GetEnv("NETWORK", "")
	defaultBeaconURIs        = common.GetSliceEnv("BEACON_URIS", []string{"http://localhost:3500"})
	defaultBeaconPublishURIs = common.GetSliceEnv("BEACON_PUBLISH_URIS", []string{})
	defaultRedisURI          = common.GetEnv("REDIS_URI", "localhost:6379")
	defaultRedisReadonlyURI  = common.GetEnv("REDIS_READONLY_URI", "")
	defaultBidEngineURI      = common.GetEnv("REDIS_BIDENGINE_URI", "")
	defaultBidEngineROURI    = common.GetEnv("REDIS_BIDENGINE_RO_URI", "")
	defaultPostgresDSN       = common.GetEnv("POSTGRES_DSN", "")
	defaultMemcachedURIs     = common.GetSliceEnv("MEMCACHED_URIS", nil)
	defaultLatencySvcURI     = common.GetEnv("LATENCY_SVC_URI", "")
	defaultLogJSON           = os.Getenv("LOG_JSON") != ""
	defaultLogLevel          = common.GetEnv("LOG_LEVEL", "info")
	defaultMevCommitRPC             = common.GetEnv("MEV_COMMIT_RPC", "")
	defaultEthereumL1RPC            = common.GetEnv("ETHEREUM_L1_RPC", "")
	defaultProviderRegistryAddr     = common.GetEnv("PROVIDER_REGISTRY_ADDR", "")
	defaultValidatorOptInRouterAddr = common.GetEnv("VALIDATOR_OPTIN_ROUTER_ADDR", "")

	enabledAPIs           []string
	beaconNodeURIs        []string
	beaconNodePublishURIs []string
	redisURI              string
	redisReadonlyURI      string
	bidEngineURI          string
	bidEngineROURI        string
	postgresDSN           string
	memcachedURIs         []string
	latencySvcURI         string

	mevCommitRPC             string
	ethereumL1RPC            string
	providerRegistryAddr     string
	validatorOptInRouterAddr string

	logJSON  bool
	logLevel string

	network string
)
