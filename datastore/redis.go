package datastore

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"math/big"
	"strconv"
	"strings"
	"time"

	builderApi "github.com/attestantio/go-builder-client/api"
	builderApiDeneb "github.com/attestantio/go-builder-client/api/deneb"
	builderSpec "github.com/attestantio/go-builder-client/spec"
	"github.com/attestantio/go-eth2-client/spec"
	"github.com/attestantio/go-eth2-client/spec/capella"
	"github.com/flashbots/go-utils/cli"
	"github.com/flashbots/mev-boost-relay/common"
	"github.com/redis/go-redis/v9"
	"github.com/flashbots/mev-boost-relay/mevcommitclient"
)

var (
	redisScheme  = "redis://"
	redisPrefix  = "boost-relay"
	bidEnginePrefix = "bid-engine"
	redisBidHash = "bid-stats"

	expiryBidCache = 45 * time.Second
	expiryLock     = 24 * time.Second

	RedisConfigFieldPubkey         = "pubkey"
	RedisStatsFieldLatestSlot      = "latest-slot"
	RedisStatsFieldValidatorsTotal = "validators-total"

	ErrFailedUpdatingTopBidNoBids            = errors.New("failed to update top bid because no bids were found")
	ErrAnotherPayloadAlreadyDeliveredForSlot = errors.New("another payload block hash for slot was already delivered")
	ErrPastSlotAlreadyDelivered              = errors.New("payload for past slot was already delivered")

	// Docs about redis settings: https://redis.io/docs/reference/clients/
	redisConnectionPoolSize = cli.GetEnvInt("REDIS_CONNECTION_POOL_SIZE", 0) // 0 means use default (10 per CPU)
	redisMinIdleConnections = cli.GetEnvInt("REDIS_MIN_IDLE_CONNECTIONS", 0) // 0 means use default
	redisReadTimeoutSec     = cli.GetEnvInt("REDIS_READ_TIMEOUT_SEC", 0)     // 0 means use default (3 sec)
	redisPoolTimeoutSec     = cli.GetEnvInt("REDIS_POOL_TIMEOUT_SEC", 0)     // 0 means use default (ReadTimeout + 1 sec)
	redisWriteTimeoutSec    = cli.GetEnvInt("REDIS_WRITE_TIMEOUT_SEC", 0)    // 0 means use default (3 seconds)
)

// Explicitly differentiate between pipeliners for different Redis clients
type RedisPipeliner interface {
	redis.Pipeliner
}

type BidEnginePipeliner interface {
	redis.Pipeliner
}

type redisPipelinerImpl struct {
	redis.Pipeliner
}

type bidEnginePipelinerImpl struct {
	redis.Pipeliner
}

func connectRedis(redisURI string) (*redis.Client, error) {
	// Handle both URIs and full URLs, assume unencrypted connections
	if !strings.HasPrefix(redisURI, redisScheme) && !strings.HasPrefix(redisURI, "rediss://") {
		redisURI = redisScheme + redisURI
	}

	redisOpts, err := redis.ParseURL(redisURI)
	if err != nil {
		return nil, err
	}

	if redisConnectionPoolSize > 0 {
		redisOpts.PoolSize = redisConnectionPoolSize
	}
	if redisMinIdleConnections > 0 {
		redisOpts.MinIdleConns = redisMinIdleConnections
	}
	if redisReadTimeoutSec > 0 {
		redisOpts.ReadTimeout = time.Duration(redisReadTimeoutSec) * time.Second
	}
	if redisPoolTimeoutSec > 0 {
		redisOpts.PoolTimeout = time.Duration(redisPoolTimeoutSec) * time.Second
	}
	if redisWriteTimeoutSec > 0 {
		redisOpts.WriteTimeout = time.Duration(redisWriteTimeoutSec) * time.Second
	}

	redisClient := redis.NewClient(redisOpts)
	if _, err := redisClient.Ping(context.Background()).Result(); err != nil {
		// unable to connect to redis
		return nil, err
	}
	return redisClient, nil
}

type RedisCache struct {
	client            *redis.Client
	readonlyClient    *redis.Client
	bidEngineClient   *redis.Client
	bidEngineROClient *redis.Client

	// prefixes (keys generated with a function)
	prefixGetHeaderResponse           string
	prefixExecPayloadCapella          string
	prefixPayloadContentsDeneb        string
	prefixPayloadContentsElectra      string
	prefixBidTrace                    string
	prefixBlockBuilderLatestBids      string // latest bid for a given slot
	prefixBlockBuilderLatestBidsValue string // value of latest bid for a given slot
	prefixBlockBuilderLatestBidsTime  string // when the request was received, to avoid older requests overwriting newer ones after a slot validation
	prefixTopBidValue                 string
	prefixFloorBid                    string
	prefixFloorBidValue               string
	prefixDeferredDemotions           string
	prefixProcessingSlot              string

	// keys
	keyValidatorRegistrationTimestamp string
	keyMevCommitBlockBuilder          string

	keyRelayConfig        string
	keyStats              string
	keyProposerDuties     string
	keyBlockBuilderStatus string
	keyLastSlotDelivered  string
	keyLastHashDelivered  string

	// channels
	channelTopBid string

	currentSlot uint64
}

func NewRedisCache(prefix, redisURI, readonlyURI, bidEngineURI, bidEngineROURI string) (*RedisCache, error) {
	client, err := connectRedis(redisURI)
	if err != nil {
		return nil, err
	}

	roClient := client
	if readonlyURI != "" {
		roClient, err = connectRedis(readonlyURI)
		if err != nil {
			return nil, err
		}
	}

	var bidEngineClient *redis.Client
	if bidEngineURI != "" {
		bidEngineClient, err = connectRedis(bidEngineURI)
		if err != nil {
			return nil, err
		}

		// Load function library
		err = bidEngineClient.FunctionLoadReplace(context.Background(), TopBidLuaLibrary).Err()
		if err != nil {
			return nil, err
		}
	}

	var bidEngineROClient *redis.Client
	if bidEngineROURI != "" {
		bidEngineROClient, err = connectRedis(bidEngineROURI)
		if err != nil {
			return nil, err
		}
	}

	return &RedisCache{
		client:         client,
		readonlyClient: roClient,
		bidEngineClient: bidEngineClient,
		bidEngineROClient: bidEngineROClient,

		prefixGetHeaderResponse:      fmt.Sprintf("%s/%s:cache-gethead-response", bidEnginePrefix, prefix),
		prefixExecPayloadCapella:     fmt.Sprintf("%s/%s:cache-execpayload-capella", redisPrefix, prefix),
		prefixPayloadContentsDeneb:   fmt.Sprintf("%s/%s:cache-payloadcontents-deneb", redisPrefix, prefix),
		prefixPayloadContentsElectra: fmt.Sprintf("%s/%s:cache-payloadcontents-electra", redisPrefix, prefix),
		prefixBidTrace:               fmt.Sprintf("%s/%s:cache-bid-trace", redisPrefix, prefix),

		prefixBlockBuilderLatestBids:      fmt.Sprintf("%s/%s:block-builder-latest-bid", bidEnginePrefix, prefix),       // hashmap for slot+parentHash+proposerPubkey with builderPubkey as field
		prefixBlockBuilderLatestBidsValue: fmt.Sprintf("%s/%s:block-builder-latest-bid-value", bidEnginePrefix, prefix), // hashmap for slot+parentHash+proposerPubkey with builderPubkey as field
		prefixBlockBuilderLatestBidsTime:  fmt.Sprintf("%s/%s:block-builder-latest-bid-time", bidEnginePrefix, prefix),  // hashmap for slot+parentHash+proposerPubkey with builderPubkey as field
		prefixTopBidValue:                 fmt.Sprintf("%s/%s:top-bid-value", bidEnginePrefix, prefix),                  // prefix:slot_parentHash_proposerPubkey
		prefixFloorBid:                    fmt.Sprintf("%s/%s:bid-floor", bidEnginePrefix, prefix),                      // prefix:slot_parentHash_proposerPubkey
		prefixFloorBidValue:               fmt.Sprintf("%s/%s:bid-floor-value", bidEnginePrefix, prefix),                // prefix:slot_parentHash_proposerPubkey
		prefixDeferredDemotions:           fmt.Sprintf("%s/%s:deferred-demotions", redisPrefix, prefix),             // prefix:blockHash
		prefixProcessingSlot:              fmt.Sprintf("%s/%s:processing-slot", redisPrefix, prefix),                // prefix:slot

		keyValidatorRegistrationTimestamp: fmt.Sprintf("%s/%s:validator-registration-timestamp", redisPrefix, prefix),
		keyMevCommitBlockBuilder:          fmt.Sprintf("%s/%s:mev-commit-block-builder", redisPrefix, prefix),
		keyRelayConfig:                    fmt.Sprintf("%s/%s:relay-config", redisPrefix, prefix),

		keyStats:              fmt.Sprintf("%s/%s:stats", redisPrefix, prefix),
		keyProposerDuties:     fmt.Sprintf("%s/%s:proposer-duties", redisPrefix, prefix),
		keyBlockBuilderStatus: fmt.Sprintf("%s/%s:block-builder-status", redisPrefix, prefix),
		keyLastSlotDelivered:  fmt.Sprintf("%s/%s:last-slot-delivered", redisPrefix, prefix),
		keyLastHashDelivered:  fmt.Sprintf("%s/%s:last-hash-delivered", redisPrefix, prefix),
		currentSlot:           0,

		channelTopBid: fmt.Sprintf("%s/%s:top-bid-updates", redisPrefix, prefix),
	}, nil
}

func (r *RedisCache) keyCacheGetHeaderResponse(slot uint64, parentHash, proposerPubkey string) string {
	return fmt.Sprintf("%s:%d_%s_%s", r.prefixGetHeaderResponse, slot, parentHash, proposerPubkey)
}

func (r *RedisCache) keyExecPayloadCapella(slot uint64, proposerPubkey, blockHash string) string {
	return fmt.Sprintf("%s:%d_%s_%s", r.prefixExecPayloadCapella, slot, proposerPubkey, blockHash)
}

func (r *RedisCache) keyPayloadContentsDeneb(slot uint64, proposerPubkey, blockHash string) string {
	return fmt.Sprintf("%s:%d_%s_%s", r.prefixPayloadContentsDeneb, slot, proposerPubkey, blockHash)
}

func (r *RedisCache) keyPayloadContentsElectra(slot uint64, proposerPubkey, blockHash string) string {
	return fmt.Sprintf("%s:%d_%s_%s", r.prefixPayloadContentsElectra, slot, proposerPubkey, blockHash)
}

func (r *RedisCache) keyCacheBidTrace(slot uint64, proposerPubkey, blockHash string) string {
	return fmt.Sprintf("%s:%d_%s_%s", r.prefixBidTrace, slot, proposerPubkey, blockHash)
}

// keyLatestBidByBuilder returns the key for the getHeader response the latest bid by a specific builder
func (r *RedisCache) keyLatestBidByBuilder(slot uint64, parentHash, proposerPubkey, builderPubkey string) string {
	return fmt.Sprintf("%s:%d_%s_%s/%s", r.prefixBlockBuilderLatestBids, slot, parentHash, proposerPubkey, builderPubkey)
}

// keyBlockBuilderLatestBidValue returns the hashmap key for the value of the latest bid by a specific builder
func (r *RedisCache) keyBlockBuilderLatestBidsValue(slot uint64, parentHash, proposerPubkey string) string {
	return fmt.Sprintf("%s:%d_%s_%s", r.prefixBlockBuilderLatestBidsValue, slot, parentHash, proposerPubkey)
}

// keyBlockBuilderLatestBidValue returns the hashmap key for the time of the latest bid by a specific builder
func (r *RedisCache) keyBlockBuilderLatestBidsTime(slot uint64, parentHash, proposerPubkey string) string {
	return fmt.Sprintf("%s:%d_%s_%s", r.prefixBlockBuilderLatestBidsTime, slot, parentHash, proposerPubkey)
}

// keyTopBidValue returns the hashmap key for the time of the latest bid by a specific builder
func (r *RedisCache) keyTopBidValue(slot uint64, parentHash, proposerPubkey string) string {
	return fmt.Sprintf("%s:%d_%s_%s", r.prefixTopBidValue, slot, parentHash, proposerPubkey)
}

// keyFloorBid returns the key for the highest non-cancellable bid of a given slot+parentHash+proposerPubkey
func (r *RedisCache) keyFloorBid(slot uint64, parentHash, proposerPubkey string) string {
	return fmt.Sprintf("%s:%d_%s_%s", r.prefixFloorBid, slot, parentHash, proposerPubkey)
}

// keyFloorBidValue returns the key for the highest non-cancellable value of a given slot+parentHash+proposerPubkey
func (r *RedisCache) keyFloorBidValue(slot uint64, parentHash, proposerPubkey string) string {
	return fmt.Sprintf("%s:%d_%s_%s", r.prefixFloorBidValue, slot, parentHash, proposerPubkey)
}

// keyDeferredDemotion returns the key for the potential deferred demotion of a given block hash
func (r *RedisCache) keyDeferredDemotion(blockHash string) string {
	return fmt.Sprintf("%s:%s", r.prefixDeferredDemotions, blockHash)
}

// keyProcessingSlot returns the key for the counter of builder processes working on a given slot
func (r *RedisCache) keyProcessingSlot(slot uint64) string {
	return fmt.Sprintf("%s:%d", r.prefixProcessingSlot, slot)
}

func (r *RedisCache) GetObj(key string, obj any) (err error) {
	var value string

	if strings.Contains(key, redisPrefix) {
		value, err = r.client.Get(context.Background(), key).Result()
	} else if strings.Contains(key, bidEnginePrefix) {
		value, err = r.bidEngineClient.Get(context.Background(), key).Result()
	} else {
		return errors.New("Invalid key prefix " + key)
	}

	if err != nil {
		return err
	}

	return json.Unmarshal([]byte(value), &obj)
}

func (r *RedisCache) SetObj(key string, value any, expiration time.Duration) (err error) {
	marshalledValue, err := json.Marshal(value)
	if err != nil {
		return err
	}

	if strings.Contains(key, redisPrefix) {
		return r.client.Set(context.Background(), key, marshalledValue, expiration).Err()
	} else if strings.Contains(key, bidEnginePrefix) {
		return r.bidEngineClient.Set(context.Background(), key, marshalledValue, expiration).Err()
	} else {
		return errors.New("Invalid key prefix " + key)
	}
}

// SetObjPipelined saves an object in the given Redis key on a Redis pipeline (JSON encoded)
func (r *RedisCache) SetObjPipelined(ctx context.Context, pipeliner redis.Pipeliner, key string, value any, expiration time.Duration) (err error) {
	marshalledValue, err := json.Marshal(value)
	if err != nil {
		return err
	}

	return pipeliner.Set(ctx, key, marshalledValue, expiration).Err()
}

func (r *RedisCache) HSetObj(key, field string, value any, expiration time.Duration) (err error) {
	marshalledValue, err := json.Marshal(value)
	if err != nil {
		return err
	}

	var client *redis.Client
	if strings.Contains(key, redisPrefix) {
		client = r.client
	} else if strings.Contains(key, bidEnginePrefix) {
		client = r.bidEngineClient
	} else {
		return errors.New("Invalid key prefix " + key)
	}

	err = client.HSet(context.Background(), key, field, marshalledValue).Err()
	if err != nil {
		return err
	}

	return client.Expire(context.Background(), key, expiration).Err()
}

func (r *RedisCache) GetValidatorRegistrationTimestamp(proposerPubkey common.PubkeyHex) (uint64, error) {
	timestamp, err := r.client.HGet(context.Background(), r.keyValidatorRegistrationTimestamp, strings.ToLower(proposerPubkey.String())).Uint64()
	if errors.Is(err, redis.Nil) {
		return 0, nil
	}
	return timestamp, err
}

func (r *RedisCache) SetMevCommitBlockBuilder(builder mevcommitclient.MevCommitProvider) error {
	ctx := context.Background()

	jsonBuilder, err := json.Marshal(builder)
	if err != nil {
		return fmt.Errorf("failed to marshal MevCommitProvider: %w", err)
	}
	err = r.client.HSet(ctx, r.keyMevCommitBlockBuilder, builder.Pubkey, string(jsonBuilder)).Err()
	if err != nil {
		return fmt.Errorf("failed to set mev-commit block builder: %w", err)
	}

	return nil
}

func (r *RedisCache) GetMevCommitBlockBuilders() ([]mevcommitclient.MevCommitProvider, error) {
	ctx := context.Background()

	// Retrieve all fields and values from the hash
	entries, err := r.client.HGetAll(ctx, r.keyMevCommitBlockBuilder).Result()
	if err != nil {
		return nil, fmt.Errorf("failed to get mev-commit block builders: %w", err)
	}

	// Convert map to MevCommitProvider slice
	builders := make([]mevcommitclient.MevCommitProvider, 0, len(entries))
	for _, value := range entries {
		var builder mevcommitclient.MevCommitProvider
		err := json.Unmarshal([]byte(value), &builder)
		if err != nil {
			return nil, fmt.Errorf("failed to unmarshal MevCommitProvider: %w", err)
		}
		builders = append(builders, builder)
	}

	return builders, nil
}

func (r *RedisCache) IsMevCommitBlockBuilder(builderPubkey common.PubkeyHex) (bool, error) {
	ctx := context.Background()

	// Check if the builder pubkey exists in the hash
	exists, err := r.client.HExists(ctx, r.keyMevCommitBlockBuilder, builderPubkey.String()).Result()
	if err != nil {
		return false, fmt.Errorf("failed to check if builder is in mev-commit block builders hash: %w", err)
	}

	return exists, nil
}

func (r *RedisCache) DeleteMevCommitBlockBuilder(builderPubkey common.PubkeyHex) error {
	ctx := context.Background()

	return r.client.HDel(ctx, r.keyMevCommitBlockBuilder, builderPubkey.String()).Err()
}

func (r *RedisCache) SetValidatorRegistrationTimestampIfNewer(proposerPubkey common.PubkeyHex, timestamp uint64) error {
	knownTimestamp, err := r.GetValidatorRegistrationTimestamp(proposerPubkey)
	if err != nil {
		return err
	}
	if knownTimestamp >= timestamp {
		return nil
	}
	return r.SetValidatorRegistrationTimestamp(proposerPubkey, timestamp)
}

func (r *RedisCache) SetValidatorRegistrationTimestamp(proposerPubkey common.PubkeyHex, timestamp uint64) error {
	return r.client.HSet(context.Background(), r.keyValidatorRegistrationTimestamp, proposerPubkey.String(), timestamp).Err()
}

func (r *RedisCache) CheckAndSetLastSlotAndHashDelivered(slot uint64, hash string) (err error) {
	// More details about Redis optimistic locking:
	// - https://redis.uptrace.dev/guide/go-redis-pipelines.html#transactions
	// - https://github.com/redis/go-redis/blob/6ecbcf6c90919350c42181ce34c1cbdfbd5d1463/race_test.go#L183
	txf := func(tx *redis.Tx) error {
		lastSlotDelivered, err := tx.Get(context.Background(), r.keyLastSlotDelivered).Uint64()
		if err != nil && !errors.Is(err, redis.Nil) {
			return err
		}

		// slot in the past, reject request
		if slot < lastSlotDelivered {
			return ErrPastSlotAlreadyDelivered
		}

		// current slot, reject request if hash is different
		if slot == lastSlotDelivered {
			lastHashDelivered, err := tx.Get(context.Background(), r.keyLastHashDelivered).Result()
			if err != nil && !errors.Is(err, redis.Nil) {
				return err
			}
			if hash != lastHashDelivered {
				return ErrAnotherPayloadAlreadyDeliveredForSlot
			}
			return nil
		}

		_, err = tx.TxPipelined(context.Background(), func(pipe redis.Pipeliner) error {
			pipe.Set(context.Background(), r.keyLastSlotDelivered, slot, 0)
			pipe.Set(context.Background(), r.keyLastHashDelivered, hash, 0)
			return nil
		})

		return err
	}

	return r.client.Watch(context.Background(), txf, r.keyLastSlotDelivered, r.keyLastHashDelivered)
}

func (r *RedisCache) GetLastSlotDelivered(ctx context.Context, pipeliner RedisPipeliner) (slot uint64, err error) {
	c := pipeliner.Get(ctx, r.keyLastSlotDelivered)
	_, err = pipeliner.Exec(ctx)
	if err != nil {
		return 0, err
	}
	return c.Uint64()
}

func (r *RedisCache) GetLastHashDelivered() (hash string, err error) {
	return r.client.Get(context.Background(), r.keyLastHashDelivered).Result()
}

func (r *RedisCache) SetStats(field string, value any) (err error) {
	return r.client.HSet(context.Background(), r.keyStats, field, value).Err()
}

func (r *RedisCache) GetStats(field string) (value string, err error) {
	return r.client.HGet(context.Background(), r.keyStats, field).Result()
}

// GetStatsUint64 returns (valueUint64, nil), or (0, redis.Nil) if the field does not exist
func (r *RedisCache) GetStatsUint64(field string) (value uint64, err error) {
	valStr, err := r.client.HGet(context.Background(), r.keyStats, field).Result()
	if err != nil {
		return 0, err
	}

	value, err = strconv.ParseUint(valStr, 10, 64)
	return value, err
}

func (r *RedisCache) SetProposerDuties(proposerDuties []common.BuilderGetValidatorsResponseEntry) (err error) {
	return r.SetObj(r.keyProposerDuties, proposerDuties, 0)
}

func (r *RedisCache) GetProposerDuties() (proposerDuties []common.BuilderGetValidatorsResponseEntry, err error) {
	proposerDuties = make([]common.BuilderGetValidatorsResponseEntry, 0)
	err = r.GetObj(r.keyProposerDuties, &proposerDuties)
	if errors.Is(err, redis.Nil) {
		return proposerDuties, nil
	}
	return proposerDuties, err
}

func (r *RedisCache) SetRelayConfig(field, value string) (err error) {
	return r.client.HSet(context.Background(), r.keyRelayConfig, field, value).Err()
}

func (r *RedisCache) GetRelayConfig(field string) (string, error) {
	res, err := r.client.HGet(context.Background(), r.keyRelayConfig, field).Result()
	if errors.Is(err, redis.Nil) {
		return res, nil
	}
	return res, err
}

func (r *RedisCache) GetBestBid(slot uint64, parentHash, proposerPubkey string) (*builderSpec.VersionedSignedBuilderBid, error) {
	key := r.keyCacheGetHeaderResponse(slot, parentHash, proposerPubkey)
	resp := new(builderSpec.VersionedSignedBuilderBid)
	err := r.GetObj(key, resp)
	if errors.Is(err, redis.Nil) {
		return nil, nil
	}
	return resp, err
}

func (r *RedisCache) GetPayloadContents(slot uint64, proposerPubkey, blockHash string) (*builderApi.VersionedSubmitBlindedBlockResponse, error) {
	resp, err := r.GetPayloadContentsElectra(slot, proposerPubkey, blockHash)
	if errors.Is(err, redis.Nil) {
		resp, err = r.GetPayloadContentsDeneb(slot, proposerPubkey, blockHash)
		if errors.Is(err, redis.Nil) {
			return r.GetExecutionPayloadCapella(slot, proposerPubkey, blockHash)
		}
	}
	return resp, err
}

func (r *RedisCache) SavePayloadContentsElectra(ctx context.Context, tx redis.Pipeliner, slot uint64, proposerPubkey, blockHash string, execPayload *builderApiDeneb.ExecutionPayloadAndBlobsBundle) (err error) {
	key := r.keyPayloadContentsElectra(slot, proposerPubkey, blockHash)
	b, err := execPayload.MarshalSSZ()
	if err != nil {
		return err
	}
	return tx.Set(ctx, key, b, expiryBidCache).Err()
}

func (r *RedisCache) GetPayloadContentsElectra(slot uint64, proposerPubkey, blockHash string) (*builderApi.VersionedSubmitBlindedBlockResponse, error) {
	electraPayloadContents := new(builderApiDeneb.ExecutionPayloadAndBlobsBundle)

	key := r.keyPayloadContentsElectra(slot, proposerPubkey, blockHash)
	val, err := r.client.Get(context.Background(), key).Result()
	if err != nil {
		return nil, err
	}

	err = electraPayloadContents.UnmarshalSSZ([]byte(val))
	if err != nil {
		return nil, err
	}

	return &builderApi.VersionedSubmitBlindedBlockResponse{
		Version: spec.DataVersionElectra,
		Electra: electraPayloadContents,
	}, nil
}

func (r *RedisCache) SavePayloadContentsDeneb(ctx context.Context, tx redis.Pipeliner, slot uint64, proposerPubkey, blockHash string, execPayload *builderApiDeneb.ExecutionPayloadAndBlobsBundle) (err error) {
	key := r.keyPayloadContentsDeneb(slot, proposerPubkey, blockHash)
	b, err := execPayload.MarshalSSZ()
	if err != nil {
		return err
	}
	return tx.Set(ctx, key, b, expiryBidCache).Err()
}

func (r *RedisCache) GetPayloadContentsDeneb(slot uint64, proposerPubkey, blockHash string) (*builderApi.VersionedSubmitBlindedBlockResponse, error) {
	denebPayloadContents := new(builderApiDeneb.ExecutionPayloadAndBlobsBundle)

	key := r.keyPayloadContentsDeneb(slot, proposerPubkey, blockHash)
	val, err := r.client.Get(context.Background(), key).Result()
	if err != nil {
		return nil, err
	}

	err = denebPayloadContents.UnmarshalSSZ([]byte(val))
	if err != nil {
		return nil, err
	}

	return &builderApi.VersionedSubmitBlindedBlockResponse{
		Version: spec.DataVersionDeneb,
		Deneb:   denebPayloadContents,
	}, nil
}

func (r *RedisCache) SaveExecutionPayloadCapella(ctx context.Context, pipeliner RedisPipeliner, slot uint64, proposerPubkey, blockHash string, execPayload *capella.ExecutionPayload) (err error) {
	key := r.keyExecPayloadCapella(slot, proposerPubkey, blockHash)
	b, err := execPayload.MarshalSSZ()
	if err != nil {
		return err
	}
	return pipeliner.Set(ctx, key, b, expiryBidCache).Err()
}

func (r *RedisCache) GetExecutionPayloadCapella(slot uint64, proposerPubkey, blockHash string) (*builderApi.VersionedSubmitBlindedBlockResponse, error) {
	capellaPayload := new(capella.ExecutionPayload)

	key := r.keyExecPayloadCapella(slot, proposerPubkey, blockHash)
	val, err := r.client.Get(context.Background(), key).Result()
	if err != nil {
		return nil, err
	}

	err = capellaPayload.UnmarshalSSZ([]byte(val))
	if err != nil {
		return nil, err
	}

	return &builderApi.VersionedSubmitBlindedBlockResponse{
		Version: spec.DataVersionCapella,
		Capella: capellaPayload,
	}, nil
}

func (r *RedisCache) SaveBidTrace(ctx context.Context, pipeliner RedisPipeliner, trace *common.BidTraceV2WithBlobFields) (err error) {
	key := r.keyCacheBidTrace(trace.Slot, trace.ProposerPubkey.String(), trace.BlockHash.String())
	return r.SetObjPipelined(ctx, pipeliner, key, trace, expiryBidCache)
}

// GetBidTrace returns (trace, nil), or (nil, redis.Nil) if the trace does not exist
func (r *RedisCache) GetBidTrace(slot uint64, proposerPubkey, blockHash string) (*common.BidTraceV2WithBlobFields, error) {
	key := r.keyCacheBidTrace(slot, proposerPubkey, blockHash)
	resp := new(common.BidTraceV2WithBlobFields)
	err := r.GetObj(key, resp)
	return resp, err
}

func (r *RedisCache) GetBuilderLatestPayloadReceivedAt(ctx context.Context, pipeliner BidEnginePipeliner, slot uint64, builderPubkey, parentHash, proposerPubkey string) (int64, error) {
	keyLatestBidsTime := r.keyBlockBuilderLatestBidsTime(slot, parentHash, proposerPubkey)
	c := pipeliner.HGet(context.Background(), keyLatestBidsTime, builderPubkey)
	_, err := pipeliner.Exec(ctx)
	if errors.Is(err, redis.Nil) {
		return 0, nil
	} else if err != nil {
		return 0, err
	}
	return c.Int64()
}

func (r *RedisCache) SaveResponses(ctx context.Context, pipeliner RedisPipeliner, payload *common.VersionedSubmitBlockRequest, submission *common.BlockSubmissionInfo, getPayloadResponse *builderApi.VersionedSubmitBlindedBlockResponse, bidTrace *common.BidTraceV2WithBlobFields) (err error) {
	// Payload contents
	switch payload.Version {
	case spec.DataVersionCapella:
		err = r.SaveExecutionPayloadCapella(ctx, pipeliner, submission.BidTrace.Slot, submission.BidTrace.ProposerPubkey.String(), submission.BidTrace.BlockHash.String(), getPayloadResponse.Capella)
		if err != nil {
			return err
		}
	case spec.DataVersionDeneb:
		err = r.SavePayloadContentsDeneb(ctx, pipeliner, submission.BidTrace.Slot, submission.BidTrace.ProposerPubkey.String(), submission.BidTrace.BlockHash.String(), getPayloadResponse.Deneb)
		if err != nil {
			return err
		}
	case spec.DataVersionElectra:
		err = r.SavePayloadContentsElectra(ctx, pipeliner, submission.BidTrace.Slot, submission.BidTrace.ProposerPubkey.String(), submission.BidTrace.BlockHash.String(), getPayloadResponse.Electra)
		if err != nil {
			return err
		}
	case spec.DataVersionUnknown, spec.DataVersionPhase0, spec.DataVersionAltair, spec.DataVersionBellatrix:
		return fmt.Errorf("unsupported payload version: %s", payload.Version) //nolint:goerr113
	}

	// Bid trace
	err = r.SaveBidTrace(ctx, pipeliner, bidTrace)
	if err != nil {
		return fmt.Errorf("failed to save bid trace: %w", err)
	}

	_, err = pipeliner.Exec(ctx)
	return err
}

type ProcessBidAndUpdateTopBidResponse struct {
	WasBidSaved      bool // Whether this bid was saved
	WasTopBidUpdated bool // Whether the top bid was updated
	IsNewTopBid      bool // Whether the submitted bid became the new top bid

	TopBidValue     *big.Int
	PrevTopBidValue *big.Int

	TimePrep         time.Duration
	TimeRedisUpdate  time.Duration
}

func (r *RedisCache) ProcessBidAndUpdateTopBid(ctx context.Context, payload *common.VersionedSubmitBlockRequest, getHeaderResponse *builderSpec.VersionedSignedBuilderBid, reqReceivedAt time.Time, isCancellationEnabled bool, floorValue *big.Int) (state ProcessBidAndUpdateTopBidResponse, err error) {
	var prevTime, nextTime time.Time
	prevTime = time.Now()

	submission, err := common.GetBlockSubmissionInfo(payload)
	if err != nil {
		return state, err
	}

	// Prepare redis function call parameters
	slot := submission.BidTrace.Slot
	parentHash := submission.BidTrace.ParentHash.String()
	proposerPubkey := submission.BidTrace.ProposerPubkey.String()
	builderPubkey := submission.BidTrace.BuilderPubkey.String()

	getHeaderResponseJson, err := json.Marshal(getHeaderResponse)
	if err != nil {
		return state, err
	}

	// Prepare redis keys and argunments
	keys := []string{
		r.keyBlockBuilderLatestBidsValue(slot, parentHash, proposerPubkey),
		r.keyFloorBidValue(slot, parentHash, proposerPubkey),
		r.keyLatestBidByBuilder(slot, parentHash, proposerPubkey, builderPubkey),
		r.keyBlockBuilderLatestBidsTime(slot, parentHash, proposerPubkey),
		r.keyCacheGetHeaderResponse(slot, parentHash, proposerPubkey),
		r.keyTopBidValue(slot, parentHash, proposerPubkey),
		r.keyFloorBid(slot, parentHash, proposerPubkey),
	}

	args := []interface{}{
		builderPubkey,
		submission.BidTrace.Value.ToBig().String(),
		fmt.Sprintf("%s:%d_%s_%s/", r.prefixBlockBuilderLatestBids, slot, parentHash, proposerPubkey),
		reqReceivedAt.UnixMilli(),
		getHeaderResponseJson,
		isCancellationEnabled,
		expiryBidCache.Seconds(),
		r.channelTopBid,
		slot,
	}

	// Record time needed for prep
	nextTime = time.Now().UTC()
	state.TimePrep = nextTime.Sub(prevTime)
	prevTime = nextTime

	// Execute redis function call
	result, err := r.bidEngineClient.FCall(ctx, LuaFunctionProcessBidAndUpdateTopBid, keys, args...).Result()
	if err != nil {
		return state, err
	}

	// Parse result
	resultSlice, ok := result.([]interface{})
	if !ok {
		return state, fmt.Errorf("unexpected result type: %T", result) //nolint:goerr113
	}

	if len(resultSlice) == 1 {
		return state, resultSlice[0].(error)
	} else if len(resultSlice) != 5 {
		return state, fmt.Errorf("unexpected number of function results: %d", len(resultSlice)) //nolint:goerr113
	}

	state.WasBidSaved = resultSlice[0].(bool)
	state.WasTopBidUpdated = resultSlice[1].(bool)
	state.IsNewTopBid = resultSlice[2].(bool)
	state.TopBidValue = new(big.Int)
	_, ok = state.TopBidValue.SetString(resultSlice[3].(string), 10)
	if !ok {
		return state, fmt.Errorf("could not set top bid value from %s", resultSlice[3].(string)) //nolint:goerr113
	}
	state.PrevTopBidValue = new(big.Int)
	_, ok = state.PrevTopBidValue.SetString(resultSlice[4].(string), 10)
	if !ok {
		return state, fmt.Errorf("could not set prev top bid value from %s", resultSlice[4].(string)) //nolint:goerr113
	}

	// Record time needed to execute function
	nextTime = time.Now().UTC()
	state.TimeRedisUpdate = nextTime.Sub(prevTime)
	prevTime = nextTime

	return state, err
}

func (r *RedisCache) updateTopBid(ctx context.Context, pipeliner BidEnginePipeliner, state ProcessBidAndUpdateTopBidResponse, builderBids *BuilderBids, slot uint64, parentHash, proposerPubkey string, floorValue *big.Int) (resp ProcessBidAndUpdateTopBidResponse, err error) {
	// Prepare redis keys and argunments
	keys := []string{
		r.keyBlockBuilderLatestBidsValue(slot, parentHash, proposerPubkey),
		r.keyFloorBidValue(slot, parentHash, proposerPubkey),
		r.keyFloorBid(slot, parentHash, proposerPubkey),
		r.keyCacheGetHeaderResponse(slot, parentHash, proposerPubkey),
		r.keyTopBidValue(slot, parentHash, proposerPubkey),
	}

	args := []interface{}{
		fmt.Sprintf("%s:%d_%s_%s/", r.prefixBlockBuilderLatestBids, slot, parentHash, proposerPubkey),
		expiryBidCache,
		r.channelTopBid,
		slot,
	}

	// Execute redis function call
	c := pipeliner.FCall(ctx, LuaFunctionUpdateTopBid, keys, args...)
	_, err = pipeliner.Exec(ctx)
	if err != nil {
		return state, err
	}
	_, err = c.Result()
	if err != nil {
		return state, err
	}

	return state, err
}

// GetTopBidValue gets the top bid value for a given slot+parent+proposer combination
func (r *RedisCache) GetTopBidValue(ctx context.Context, pipeliner BidEnginePipeliner, slot uint64, parentHash, proposerPubkey string) (topBidValue *big.Int, err error) {
	keyTopBidValue := r.keyTopBidValue(slot, parentHash, proposerPubkey)
	c := pipeliner.Get(ctx, keyTopBidValue)
	_, err = pipeliner.Exec(ctx)
	if errors.Is(err, redis.Nil) {
		return big.NewInt(0), nil
	} else if err != nil {
		return nil, err
	}

	topBidValueStr, err := c.Result()
	if err != nil {
		return nil, err
	}
	topBidValue = new(big.Int)
	topBidValue, ok := topBidValue.SetString(topBidValueStr, 10)
	if !ok {
		return nil, fmt.Errorf("could not set top bid value from %s", topBidValueStr) //nolint:goerr113
	}
	return topBidValue, nil
}

// GetBuilderLatestValue gets the latest bid value for a given slot+parent+proposer combination for a specific builder pubkey.
func (r *RedisCache) GetBuilderLatestValue(slot uint64, parentHash, proposerPubkey, builderPubkey string) (topBidValue *big.Int, err error) {
	keyLatestValue := r.keyBlockBuilderLatestBidsValue(slot, parentHash, proposerPubkey)
	topBidValueStr, err := r.bidEngineClient.HGet(context.Background(), keyLatestValue, builderPubkey).Result()
	if errors.Is(err, redis.Nil) {
		return big.NewInt(0), nil
	} else if err != nil {
		return nil, err
	}
	topBidValue = new(big.Int)
	topBidValue, ok := topBidValue.SetString(topBidValueStr, 10)
	if !ok {
		return nil, fmt.Errorf("could not set top bid value from %s", topBidValueStr) //nolint:goerr113
	}
	return topBidValue, nil
}

// DelBuilderBid removes a builders most recent bid
func (r *RedisCache) DelBuilderBid(ctx context.Context, slot uint64, parentHash, proposerPubkey, builderPubkey string) (err error) {
	// Create a tx pipeline for these operations
	pipeliner := r.NewTxBidEnginePipeline()

	// delete the value
	keyLatestValue := r.keyBlockBuilderLatestBidsValue(slot, parentHash, proposerPubkey)
	err = pipeliner.HDel(ctx, keyLatestValue, builderPubkey).Err()
	if err != nil && !errors.Is(err, redis.Nil) {
		return err
	}

	// delete the time
	keyLatestBidsTime := r.keyBlockBuilderLatestBidsTime(slot, parentHash, proposerPubkey)
	err = pipeliner.HDel(ctx, keyLatestBidsTime, builderPubkey).Err()
	if err != nil {
		return err
	}

	// update bids now to compute current top bid
	// Executes the pipeline
	state := ProcessBidAndUpdateTopBidResponse{} //nolint:exhaustruct
	_, err = r.updateTopBid(ctx, pipeliner, state, nil, slot, parentHash, proposerPubkey, nil)
	return err
}

// GetFloorBidValue returns the value of the highest non-cancellable bid
func (r *RedisCache) GetFloorBidValue(ctx context.Context, pipeliner BidEnginePipeliner, slot uint64, parentHash, proposerPubkey string) (floorValue *big.Int, err error) {
	keyFloorBidValue := r.keyFloorBidValue(slot, parentHash, proposerPubkey)
	c := pipeliner.Get(ctx, keyFloorBidValue)

	_, err = pipeliner.Exec(ctx)
	if errors.Is(err, redis.Nil) {
		return big.NewInt(0), nil
	} else if err != nil {
		return nil, err
	}

	topBidValueStr, err := c.Result()
	if err != nil {
		return nil, err
	}
	floorValue = new(big.Int)
	floorValue.SetString(topBidValueStr, 10)
	return floorValue, nil
}

// SetFloorBidValue is used only for testing.
func (r *RedisCache) SetFloorBidValue(slot uint64, parentHash, proposerPubkey, value string) error {
	keyFloorBidValue := r.keyFloorBidValue(slot, parentHash, proposerPubkey)
	err := r.bidEngineClient.Set(context.Background(), keyFloorBidValue, value, 0).Err()
	return err
}

func (r *RedisCache) SaveDeferredDemotion(blockHash string, demote *common.DemotionResult) (err error) {
	key := r.keyDeferredDemotion(blockHash)
	return r.SetObj(key, demote, expiryBidCache)
}

// GetDeferredDemotion returns (demote, nil), or (nil, redis.Nil) if there is no deferred demotion
func (r *RedisCache) GetDeferredDemotion(blockHash string) (*common.DemotionResult, error) {
	key := r.keyDeferredDemotion(blockHash)
	resp := new(common.DemotionResult)
	err := r.GetObj(key, resp)
	return resp, err
}

// BeginProcessingSlot signals that a builder process is handling blocks for a given slot
func (r *RedisCache) BeginProcessingSlot(ctx context.Context, slot uint64) (err error) {
	// Should never process more than one slot at a time
	if r.currentSlot != 0 {
		return fmt.Errorf("already processing slot %d", r.currentSlot) //nolint:goerr113
	}

	keyProcessingSlot := r.keyProcessingSlot(slot)

	pipe := r.NewTxPipeline()
	pipe.Incr(ctx, keyProcessingSlot)
	pipe.Expire(ctx, keyProcessingSlot, expiryLock)
	_, err = pipe.Exec(ctx)

	if err != nil {
		return err
	}

	r.currentSlot = slot
	return nil
}

// EndProcessingSlot signals that a builder process is done handling blocks for the current slot
func (r *RedisCache) EndProcessingSlot(ctx context.Context) (err error) {
	// Do not decrement if called multiple times
	if r.currentSlot == 0 {
		return nil
	}

	keyProcessingSlot := r.keyProcessingSlot(r.currentSlot)

	pipe := r.NewTxPipeline()
	pipe.Decr(ctx, keyProcessingSlot)
	pipe.Expire(ctx, keyProcessingSlot, expiryLock)
	_, err = pipe.Exec(ctx)

	if err != nil {
		return err
	}

	r.currentSlot = 0
	return nil
}

// WaitForSlotComplete waits for a slot to be completed by all builder processes
func (r *RedisCache) WaitForSlotComplete(ctx context.Context, slot uint64) (err error) {
	keyProcessingSlot := r.keyProcessingSlot(slot)
	for {
		processing, err := r.client.Get(ctx, keyProcessingSlot).Uint64()
		if err != nil {
			return err
		}
		if processing == 0 {
			return nil
		}
		time.Sleep(50 * time.Millisecond)
	}
}

func (r *RedisCache) NewPipeline() redis.Pipeliner { //nolint:ireturn,nolintlint
	return r.client.Pipeline()
}

func (r *RedisCache) NewTxPipeline() RedisPipeliner { //nolint:ireturn
	return &redisPipelinerImpl{r.client.TxPipeline()}
}

func (r *RedisCache) NewTxBidEnginePipeline() BidEnginePipeliner { //nolint:ireturn
	return &bidEnginePipelinerImpl{r.bidEngineClient.TxPipeline()}
}

func (r *RedisCache) NewTxBidEngineROPipeline() BidEnginePipeliner { //nolint:ireturn
	return &bidEnginePipelinerImpl{r.bidEngineROClient.TxPipeline()}
}
