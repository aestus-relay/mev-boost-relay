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

	"github.com/attestantio/go-builder-client/api"
	consensusspec "github.com/attestantio/go-eth2-client/spec"
	"github.com/attestantio/go-eth2-client/spec/capella"
	boostTypes "github.com/flashbots/go-boost-utils/types"
	"github.com/flashbots/go-utils/cli"
	"github.com/flashbots/mev-boost-relay/common"
	"github.com/go-redis/redis/v9"
	"github.com/hedhyw/otelinji/pkg/otelinji"
	"go.opentelemetry.io/otel"
)

var (
	redisPrefix  = "boost-relay"
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

func PubkeyHexToLowerStr(pk boostTypes.PubkeyHex) string {
	return strings.ToLower(string(pk))
}

func connectRedis(redisURIs []string, redisPassword string) (*redis.ClusterClient, error) {
	clusterOpt := &redis.ClusterOptions{
		Addrs:    redisURIs,
		Password: redisPassword,
		ReadOnly: true,
	}

	if redisConnectionPoolSize > 0 {
		clusterOpt.PoolSize = redisConnectionPoolSize
	}
	if redisMinIdleConnections > 0 {
		clusterOpt.MinIdleConns = redisMinIdleConnections
	}
	if redisReadTimeoutSec > 0 {
		clusterOpt.ReadTimeout = time.Duration(redisReadTimeoutSec) * time.Second
	}
	if redisPoolTimeoutSec > 0 {
		clusterOpt.PoolTimeout = time.Duration(redisPoolTimeoutSec) * time.Second
	}
	if redisWriteTimeoutSec > 0 {
		clusterOpt.WriteTimeout = time.Duration(redisWriteTimeoutSec) * time.Second
	}

	redisClient := redis.NewClusterClient(clusterOpt)
	if _, err := redisClient.Ping(context.Background()).Result(); err != nil {
		// unable to connect to redis
		return nil, err
	}
	return redisClient, nil
}

type RedisCache struct {
	client *redis.ClusterClient

	// prefixes (keys generated with a function)
	prefixGetHeaderResponse           string
	prefixExecPayloadCapella          string
	prefixBidTrace                    string
	prefixBlockBuilderLatestBids      string // latest bid for a given slot
	prefixBlockBuilderLatestBidsValue string // value of latest bid for a given slot
	prefixBlockBuilderLatestBidsTime  string // when the request was received, to avoid older requests overwriting newer ones after a slot validation
	prefixTopBidValue                 string
	prefixFloorBid                    string
	prefixFloorBidValue               string
	prefixProcessingSlot              string
	prefixDeferredDemotions           string

	// keys
	keyValidatorRegistrationTimestamp string

	keyRelayConfig        string
	keyStats              string
	keyProposerDuties     string
	keyBlockBuilderStatus string
	keyLastSlotDelivered  string
	keyLastHashDelivered  string

	currentSlot uint64
}

func NewRedisCache(prefix string, redisURIs []string, redisPassword string) (*RedisCache, error) {
	client, err := connectRedis(redisURIs, redisPassword)

	if err != nil {
		return nil, err
	}

	return &RedisCache{
		client: client,

		prefixGetHeaderResponse:  fmt.Sprintf("%s/%s:cache-gethead-response", redisPrefix, prefix),
		prefixExecPayloadCapella: fmt.Sprintf("%s/%s:cache-execpayload-capella", redisPrefix, prefix),
		prefixBidTrace:           fmt.Sprintf("%s/%s:cache-bid-trace", redisPrefix, prefix),

		prefixBlockBuilderLatestBids:      fmt.Sprintf("%s/%s:block-builder-latest-bid", redisPrefix, prefix),       // hashmap for slot+parentHash+proposerPubkey with builderPubkey as field
		prefixBlockBuilderLatestBidsValue: fmt.Sprintf("%s/%s:block-builder-latest-bid-value", redisPrefix, prefix), // hashmap for slot+parentHash+proposerPubkey with builderPubkey as field
		prefixBlockBuilderLatestBidsTime:  fmt.Sprintf("%s/%s:block-builder-latest-bid-time", redisPrefix, prefix),  // hashmap for slot+parentHash+proposerPubkey with builderPubkey as field
		prefixTopBidValue:                 fmt.Sprintf("%s/%s:top-bid-value", redisPrefix, prefix),                  // prefix:slot_parentHash_proposerPubkey
		prefixFloorBid:                    fmt.Sprintf("%s/%s:bid-floor", redisPrefix, prefix),                      // prefix:slot_parentHash_proposerPubkey
		prefixFloorBidValue:               fmt.Sprintf("%s/%s:bid-floor-value", redisPrefix, prefix),                // prefix:slot_parentHash_proposerPubkey
		prefixProcessingSlot:              fmt.Sprintf("%s/%s:processing-slot", redisPrefix, prefix),                // prefix:slot
		prefixDeferredDemotions:           fmt.Sprintf("%s/%s:deferred-demotions", redisPrefix, prefix),             // prefix:blockHash

		keyValidatorRegistrationTimestamp: fmt.Sprintf("%s/%s:validator-registration-timestamp", redisPrefix, prefix),
		keyRelayConfig:                    fmt.Sprintf("%s/%s:relay-config", redisPrefix, prefix),

		keyStats:              fmt.Sprintf("%s/%s:stats", redisPrefix, prefix),
		keyProposerDuties:     fmt.Sprintf("%s/%s:proposer-duties", redisPrefix, prefix),
		keyBlockBuilderStatus: fmt.Sprintf("%s/%s:block-builder-status", redisPrefix, prefix),
		keyLastSlotDelivered:  fmt.Sprintf("%s/%s#{%s}:last-slot-delivered", redisPrefix, prefix, redisBidHash),
		keyLastHashDelivered:  fmt.Sprintf("%s/%s#{%s}:last-hash-delivered", redisPrefix, prefix, redisBidHash),
		currentSlot:           0,
	}, nil
}

func (r *RedisCache) keyCacheGetHeaderResponse(slot uint64, parentHash, proposerPubkey string) string {
	return fmt.Sprintf("%s:%d_%s_%s", r.prefixGetHeaderResponse, slot, parentHash, proposerPubkey)
}

func (r *RedisCache) keyExecPayloadCapella(slot uint64, proposerPubkey, blockHash string) string {
	return fmt.Sprintf("%s:%d_%s_%s", r.prefixExecPayloadCapella, slot, proposerPubkey, blockHash)
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

// keyProcessingSlot returns the key for the counter of builder processes working on a given slot
func (r *RedisCache) keyProcessingSlot(slot uint64) string {
	return fmt.Sprintf("%s:%d", r.prefixProcessingSlot, slot)
}

// keyDeferredDemotion returns the key for the potential deferred demotion of a given block hash
func (r *RedisCache) keyDeferredDemotion(blockHash string) string {
	return fmt.Sprintf("%s:%s", r.prefixDeferredDemotions, blockHash)
}

func (r *RedisCache) GetObj(key string, obj any) (err error) {
	value, err := r.client.Get(context.Background(), key).Result()
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

	return r.client.Set(context.Background(), key, marshalledValue, expiration).Err()
}

// SetObjPipelined saves an object in the given Redis key on a Redis pipeline (JSON encoded)

func (r *RedisCache) SetObjPipelined(ctx context.Context, pipeliner redis.Pipeliner, key string, value any, expiration time.Duration) (err error) {
	ctx, span := otel.Tracer("datastore").Start(ctx, "RedisCache.SetObjPipelined")
	defer func() { otelinji.EndSpanWithErr(span, err) }()

	if pipeliner == nil {
		return r.SetObj(key, value, expiration)
	}

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

	err = r.client.HSet(context.Background(), key, field, marshalledValue).Err()
	if err != nil {
		return err
	}

	return r.client.Expire(context.Background(), key, expiration).Err()
}

func (r *RedisCache) GetValidatorRegistrationTimestamp(proposerPubkey boostTypes.PubkeyHex) (uint64, error) {
	timestamp, err := r.client.HGet(context.Background(), r.keyValidatorRegistrationTimestamp, strings.ToLower(proposerPubkey.String())).Uint64()
	if errors.Is(err, redis.Nil) {
		return 0, nil
	}
	return timestamp, err
}

func (r *RedisCache) SetValidatorRegistrationTimestampIfNewer(proposerPubkey boostTypes.PubkeyHex, timestamp uint64) error {
	knownTimestamp, err := r.GetValidatorRegistrationTimestamp(proposerPubkey)
	if err != nil {
		return err
	}
	if knownTimestamp >= timestamp {
		return nil
	}
	return r.SetValidatorRegistrationTimestamp(proposerPubkey, timestamp)
}

func (r *RedisCache) SetValidatorRegistrationTimestamp(proposerPubkey boostTypes.PubkeyHex, timestamp uint64) error {
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

func (r *RedisCache) GetLastSlotDelivered(ctx context.Context, pipeliner redis.Pipeliner) (slot uint64, err error) {
	ctx, span := otel.Tracer("datastore").Start(ctx, "RedisCache.GetLastSlotDelivered")
	defer func() { otelinji.EndSpanWithErr(span, err) }()

	c, err := r.GetPL(ctx, pipeliner, r.keyLastSlotDelivered)
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

func (r *RedisCache) GetBestBid(slot uint64, parentHash, proposerPubkey string) (*common.GetHeaderResponse, error) {
	key := r.keyCacheGetHeaderResponse(slot, parentHash, proposerPubkey)
	resp := new(common.GetHeaderResponse)
	err := r.GetObj(key, resp)
	if errors.Is(err, redis.Nil) {
		return nil, nil
	}
	return resp, err
}

func (r *RedisCache) SaveExecutionPayloadCapella(ctx context.Context, pipeliner redis.Pipeliner, slot uint64, proposerPubkey, blockHash string, execPayload *capella.ExecutionPayload) (err error) {
	ctx, span := otel.Tracer("datastore").Start(ctx, "RedisCache.SaveExecutionPayloadCapella")
	defer func() { otelinji.EndSpanWithErr(span, err) }()

	key := r.keyExecPayloadCapella(slot, proposerPubkey, blockHash)
	b, err := execPayload.MarshalSSZ()
	if err != nil {
		return err
	}
	return r.SetPL(ctx, pipeliner, key, b, expiryBidCache)
}

func (r *RedisCache) GetExecutionPayloadCapella(slot uint64, proposerPubkey, blockHash string) (*common.VersionedExecutionPayload, error) {
	resp := new(common.VersionedExecutionPayload)
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

	resp.Capella = new(api.VersionedExecutionPayload)
	resp.Capella.Capella = capellaPayload
	resp.Capella.Version = consensusspec.DataVersionCapella
	return resp, nil
}

func (r *RedisCache) SaveBidTrace(ctx context.Context, pipeliner redis.Pipeliner, trace *common.BidTraceV2) (err error) {
	ctx, span := otel.Tracer("datastore").Start(ctx, "RedisCache.SaveBidTrace")
	defer func() { otelinji.EndSpanWithErr(span, err) }()

	key := r.keyCacheBidTrace(trace.Slot, trace.ProposerPubkey.String(), trace.BlockHash.String())
	return r.SetObjPipelined(ctx, pipeliner, key, trace, expiryBidCache)
}

// GetBidTrace returns (trace, nil), or (nil, redis.Nil) if the trace does not exist
func (r *RedisCache) GetBidTrace(slot uint64, proposerPubkey, blockHash string) (*common.BidTraceV2, error) {
	key := r.keyCacheBidTrace(slot, proposerPubkey, blockHash)
	resp := new(common.BidTraceV2)
	err := r.GetObj(key, resp)
	return resp, err
}

func (r *RedisCache) GetBuilderLatestPayloadReceivedAt(ctx context.Context, pipeliner redis.Pipeliner, slot uint64, builderPubkey, parentHash, proposerPubkey string) (int64, error) {
	ctx, span := otel.Tracer("datastore").Start(ctx, "RedisCache.GetBuilderLatestPayloadReceivedAt")
	defer span.End()

	_ = ctx

	keyLatestBidsTime := r.keyBlockBuilderLatestBidsTime(slot, parentHash, proposerPubkey)
	c, err := r.HGetPL(context.Background(), pipeliner, keyLatestBidsTime, builderPubkey)
	if errors.Is(err, redis.Nil) {
		return 0, nil
	} else if err != nil {
		return 0, err
	}
	return c.Int64()
}

// SaveBuilderBid saves the latest bid by a specific builder. TODO: use transaction to make these writes atomic
func (r *RedisCache) SaveBuilderBid(ctx context.Context, pipeliner redis.Pipeliner, slot uint64, parentHash, proposerPubkey, builderPubkey string, receivedAt time.Time, headerResp *common.GetHeaderResponse) (err error) {
	ctx, span := otel.Tracer("datastore").Start(ctx, "RedisCache.SaveBuilderBid")
	defer func() { otelinji.EndSpanWithErr(span, err) }()

	// save the actual bid
	keyLatestBid := r.keyLatestBidByBuilder(slot, parentHash, proposerPubkey, builderPubkey)
	err = r.SetObjPipelined(ctx, pipeliner, keyLatestBid, headerResp, expiryBidCache)
	if err != nil {
		return err
	}

	// set the time of the request
	keyLatestBidsTime := r.keyBlockBuilderLatestBidsTime(slot, parentHash, proposerPubkey)
	err = r.HSetPL(ctx, pipeliner, keyLatestBidsTime, builderPubkey, receivedAt.UnixMilli())
	if err != nil {
		return err
	}
	err = r.ExpirePL(ctx, pipeliner, keyLatestBidsTime, expiryBidCache)
	if err != nil {
		return err
	}

	// set the value last, because that's iterated over when updating the best bid, and the payload has to be available
	keyLatestBidsValue := r.keyBlockBuilderLatestBidsValue(slot, parentHash, proposerPubkey)
	err = r.HSetPL(ctx, pipeliner, keyLatestBidsValue, builderPubkey, headerResp.Value().String())
	if err != nil {
		return err
	}
	return r.ExpirePL(ctx, pipeliner, keyLatestBidsValue, expiryBidCache)
}

type SaveBidAndUpdateTopBidResponse struct {
	WasBidSaved      bool // Whether this bid was saved
	WasTopBidUpdated bool // Whether the top bid was updated
	IsNewTopBid      bool // Whether the submitted bid became the new top bid

	TopBidValue     *big.Int
	PrevTopBidValue *big.Int

	TimePrep         time.Duration
	TimeSavePayload  time.Duration
	TimeSaveBid      time.Duration
	TimeSaveTrace    time.Duration
	TimeUpdateTopBid time.Duration
	TimeUpdateFloor  time.Duration
}

func (r *RedisCache) SaveBidAndUpdateTopBid(ctx context.Context, pipeliner redis.Pipeliner, trace *common.BidTraceV2, payload *common.BuilderSubmitBlockRequest, getPayloadResponse *common.GetPayloadResponse, getHeaderResponse *common.GetHeaderResponse, reqReceivedAt time.Time, isCancellationEnabled bool, floorValue *big.Int) (state SaveBidAndUpdateTopBidResponse, err error) {
	ctx, span := otel.Tracer("datastore").Start(ctx, "RedisCache.SaveBidAndUpdateTopBid")
	defer func() { otelinji.EndSpanWithErr(span, err) }()

	var prevTime, nextTime time.Time
	prevTime = time.Now()

	// Load latest bids for a given slot+parent+proposer
	builderBids, err := NewBuilderBidsFromRedis(ctx, r, pipeliner, payload.Slot(), payload.ParentHash(), payload.ProposerPubkey())
	if err != nil {
		return state, err
	}

	// Load floor value (if not passed in already)
	if floorValue == nil {
		floorValue, err = r.GetFloorBidValue(ctx, pipeliner, payload.Slot(), payload.ParentHash(), payload.ProposerPubkey())
		if err != nil {
			return state, err
		}
	}

	// Get the reference top bid value
	_, state.TopBidValue = builderBids.getTopBid()
	if floorValue.Cmp(state.TopBidValue) == 1 {
		state.TopBidValue = floorValue
	}
	state.PrevTopBidValue = state.TopBidValue

	// Abort now if non-cancellation bid is lower than floor value
	isBidAboveFloor := payload.Value().Cmp(floorValue) == 1
	if !isCancellationEnabled && !isBidAboveFloor {
		return state, nil
	}

	// Record time needed
	nextTime = time.Now().UTC()
	state.TimePrep = nextTime.Sub(prevTime)
	prevTime = nextTime

	//
	// Time to save things in Redis
	//
	// 1. Save the execution payload
	err = r.SaveExecutionPayloadCapella(ctx, pipeliner, payload.Slot(), payload.ProposerPubkey(), payload.BlockHash(), getPayloadResponse.Capella.Capella)
	if err != nil {
		return state, err
	}

	// Record time needed to save payload
	nextTime = time.Now().UTC()
	state.TimeSavePayload = nextTime.Sub(prevTime)
	prevTime = nextTime

	// 2. Save latest bid for this builder
	err = r.SaveBuilderBid(ctx, pipeliner, payload.Slot(), payload.ParentHash(), payload.ProposerPubkey(), payload.BuilderPubkey().String(), reqReceivedAt, getHeaderResponse)
	if err != nil {
		return state, err
	}
	builderBids.bidValues[payload.BuilderPubkey().String()] = payload.Value()

	// Record time needed to save bid
	nextTime = time.Now().UTC()
	state.TimeSaveBid = nextTime.Sub(prevTime)
	prevTime = nextTime

	// 3. Save the bid trace
	err = r.SaveBidTrace(ctx, pipeliner, trace)
	if err != nil {
		return state, err
	}

	// Record time needed to save trace
	nextTime = time.Now().UTC()
	state.TimeSaveTrace = nextTime.Sub(prevTime)
	prevTime = nextTime

	// If top bid value hasn't change, abort now
	_, state.TopBidValue = builderBids.getTopBid()
	if state.TopBidValue.Cmp(state.PrevTopBidValue) == 0 {
		return state, nil
	}

	state, err = r._updateTopBid(ctx, pipeliner, state, builderBids, payload.Slot(), payload.ParentHash(), payload.ProposerPubkey(), floorValue)
	if err != nil {
		return state, err
	}
	state.IsNewTopBid = payload.Value().Cmp(state.TopBidValue) == 0
	// An Exec happens in _updateTopBid.
	state.WasBidSaved = true

	// Record time needed to update top bid
	nextTime = time.Now().UTC()
	state.TimeUpdateTopBid = nextTime.Sub(prevTime)
	prevTime = nextTime

	if isCancellationEnabled || !isBidAboveFloor {
		return state, nil
	}

	// Non-cancellable bid above floor should set new floor
	keyBidSource := r.keyLatestBidByBuilder(payload.Slot(), payload.ParentHash(), payload.ProposerPubkey(), payload.BuilderPubkey().String())
	keyFloorBid := r.keyFloorBid(payload.Slot(), payload.ParentHash(), payload.ProposerPubkey())
	resp := new(common.GetHeaderResponse)
	err = r.CopyPL(ctx, pipeliner, keyBidSource, keyFloorBid, 0, true, resp)
	if err != nil {
		return state, err
	}
	err = r.ExpirePL(ctx, pipeliner, keyFloorBid, expiryBidCache)
	if err != nil {
		return state, err
	}

	keyFloorBidValue := r.keyFloorBidValue(payload.Slot(), payload.ParentHash(), payload.ProposerPubkey())
	err = r.SetPL(ctx, pipeliner, keyFloorBidValue, payload.Value().String(), expiryBidCache)
	if err != nil {
		return state, err
	}

	// Execute setting the floor bid
	err = r.ExecPL(ctx, pipeliner)

	// Record time needed to update floor
	nextTime = time.Now().UTC()
	state.TimeUpdateFloor = nextTime.Sub(prevTime)

	return state, err
}

func (r *RedisCache) _updateTopBid(ctx context.Context, pipeliner redis.Pipeliner, state SaveBidAndUpdateTopBidResponse, builderBids *BuilderBids, slot uint64, parentHash, proposerPubkey string, floorValue *big.Int) (resp SaveBidAndUpdateTopBidResponse, err error) {
	if builderBids == nil {
		builderBids, err = NewBuilderBidsFromRedis(ctx, r, pipeliner, slot, parentHash, proposerPubkey)
		if err != nil {
			return state, err
		}
	}

	if len(builderBids.bidValues) == 0 {
		return state, nil
	}

	// Load floor value (if not passed in already)
	if floorValue == nil {
		floorValue, err = r.GetFloorBidValue(ctx, pipeliner, slot, parentHash, proposerPubkey)
		if err != nil {
			return state, err
		}
	}

	topBidBuilder := ""
	topBidBuilder, state.TopBidValue = builderBids.getTopBid()
	keyBidSource := r.keyLatestBidByBuilder(slot, parentHash, proposerPubkey, topBidBuilder)

	// If floor value is higher than this bid, use floor bid instead
	if floorValue.Cmp(state.TopBidValue) == 1 {
		state.TopBidValue = floorValue
		keyBidSource = r.keyFloorBid(slot, parentHash, proposerPubkey)
	}

	// Copy winning bid to top bid cache
	keyTopBid := r.keyCacheGetHeaderResponse(slot, parentHash, proposerPubkey)
	intermediate := new(common.GetHeaderResponse)
	err = r.CopyPL(context.Background(), pipeliner, keyBidSource, keyTopBid, 0, true, intermediate)
	if err != nil {
		return state, err
	}
	err = r.ExpirePL(context.Background(), pipeliner, keyTopBid, expiryBidCache)
	if err != nil {
		return state, err
	}

	state.WasTopBidUpdated = state.PrevTopBidValue == nil || state.PrevTopBidValue.Cmp(state.TopBidValue) != 0

	// 6. Finally, update the global top bid value
	keyTopBidValue := r.keyTopBidValue(slot, parentHash, proposerPubkey)
	err = r.SetPL(context.Background(), pipeliner, keyTopBidValue, state.TopBidValue.String(), expiryBidCache)
	if err != nil {
		return state, err
	}

	err = r.ExecPL(ctx, pipeliner)
	return state, err
}

// GetTopBidValue gets the top bid value for a given slot+parent+proposer combination
func (r *RedisCache) GetTopBidValue(ctx context.Context, pipeliner redis.Pipeliner, slot uint64, parentHash, proposerPubkey string) (topBidValue *big.Int, err error) {
	ctx, span := otel.Tracer("datastore").Start(ctx, "RedisCache.GetTopBidValue")
	defer func() { otelinji.EndSpanWithErr(span, err) }()

	keyTopBidValue := r.keyTopBidValue(slot, parentHash, proposerPubkey)
	c, err := r.GetPL(ctx, pipeliner, keyTopBidValue)
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
	topBidValue.SetString(topBidValueStr, 10)
	return topBidValue, nil
}

// GetBuilderLatestValue gets the latest bid value for a given slot+parent+proposer combination for a specific builder pubkey.
func (r *RedisCache) GetBuilderLatestValue(slot uint64, parentHash, proposerPubkey, builderPubkey string) (topBidValue *big.Int, err error) {
	keyLatestValue := r.keyBlockBuilderLatestBidsValue(slot, parentHash, proposerPubkey)
	topBidValueStr, err := r.client.HGet(context.Background(), keyLatestValue, builderPubkey).Result()
	if errors.Is(err, redis.Nil) {
		return big.NewInt(0), nil
	} else if err != nil {
		return nil, err
	}
	topBidValue = new(big.Int)
	topBidValue.SetString(topBidValueStr, 10)
	return topBidValue, nil
}

// DelBuilderBid removes a builders most recent bid
func (r *RedisCache) DelBuilderBid(ctx context.Context, pipeliner redis.Pipeliner, slot uint64, parentHash, proposerPubkey, builderPubkey string) (err error) {
	ctx, span := otel.Tracer("datastore").Start(ctx, "RedisCache.DelBuilderBid")
	defer func() { otelinji.EndSpanWithErr(span, err) }()

	// delete the value
	keyLatestValue := r.keyBlockBuilderLatestBidsValue(slot, parentHash, proposerPubkey)
	err = r.client.HDel(ctx, keyLatestValue, builderPubkey).Err()
	if err != nil && !errors.Is(err, redis.Nil) {
		return err
	}

	// delete the time
	keyLatestBidsTime := r.keyBlockBuilderLatestBidsTime(slot, parentHash, proposerPubkey)
	err = r.client.HDel(ctx, keyLatestBidsTime, builderPubkey).Err()
	if err != nil {
		return err
	}

	// update bids now to compute current top bid
	state := SaveBidAndUpdateTopBidResponse{} //nolint:exhaustruct
	_, err = r._updateTopBid(ctx, pipeliner, state, nil, slot, parentHash, proposerPubkey, nil)
	return err
}

// GetFloorBidValue returns the value of the highest non-cancellable bid
func (r *RedisCache) GetFloorBidValue(ctx context.Context, pipeliner redis.Pipeliner, slot uint64, parentHash, proposerPubkey string) (floorValue *big.Int, err error) {
	ctx, span := otel.Tracer("datastore").Start(ctx, "RedisCache.GetFloorBidValue")
	defer func() { otelinji.EndSpanWithErr(span, err) }()

	keyFloorBidValue := r.keyFloorBidValue(slot, parentHash, proposerPubkey)
	c, err := r.GetPL(ctx, pipeliner, keyFloorBidValue)
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
	err := r.client.Set(context.Background(), keyFloorBidValue, value, 0).Err()
	return err
}

// BeginProcessingSlot signals that a builder process is handling blocks for a given slot
func (r *RedisCache) BeginProcessingSlot(ctx context.Context, slot uint64) (err error) {
	ctx, span := otel.Tracer("datastore").Start(ctx, "RedisCache.BeginProcessingSlot")
	defer func() { otelinji.EndSpanWithErr(span, err) }()

	// Should never process more than one slot at a time
	if r.currentSlot != 0 {
		return fmt.Errorf("already processing slot %d", r.currentSlot) //nolint:goerr113
	}

	keyProcessingSlot := r.keyProcessingSlot(slot)
	err = r.client.Incr(ctx, keyProcessingSlot).Err()
	if err != nil {
		return err
	}
	r.currentSlot = slot
	err = r.client.Expire(ctx, keyProcessingSlot, expiryLock).Err()
	return err
}

// EndProcessingSlot signals that a builder process is done handling blocks for the current slot
func (r *RedisCache) EndProcessingSlot(ctx context.Context) (err error) {
	ctx, span := otel.Tracer("datastore").Start(ctx, "RedisCache.EndProcessingSlot")
	defer func() { otelinji.EndSpanWithErr(span, err) }()

	// Do not decrement if called multiple times
	if r.currentSlot == 0 {
		return nil
	}

	keyProcessingSlot := r.keyProcessingSlot(r.currentSlot)
	err = r.client.Decr(ctx, keyProcessingSlot).Err()
	r.currentSlot = 0
	return err
}

// WaitForSlotComplete waits for a slot to be completed by all builder processes
func (r *RedisCache) WaitForSlotComplete(ctx context.Context, slot uint64) (err error) {
	ctx, span := otel.Tracer("datastore").Start(ctx, "RedisCache.WaitForSlotComplete")
	defer func() { otelinji.EndSpanWithErr(span, err) }()

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

func (r *RedisCache) SaveDeferredDemotion(blockHash string, demote *common.DemotionResult) (err error) {
	key := r.keyDeferredDemotion(blockHash)
	return r.SetObj(key, demote, expiryBidCache)
}

// GetDemotionResult returns (demote, nil), or (nil, redis.Nil) if there is no deferred demotion
func (r *RedisCache) GetDeferredDemotion(blockHash string) (*common.DemotionResult, error) {
	key := r.keyDeferredDemotion(blockHash)
	resp := new(common.DemotionResult)
	err := r.GetObj(key, resp)
	return resp, err
}

func (r *RedisCache) NewPipeline() redis.Pipeliner { //nolint:ireturn,nolintlint
	return r.client.Pipeline()
}

func (r *RedisCache) NewTxPipeline() redis.Pipeliner { //nolint:ireturn
	return r.client.TxPipeline()
}

func (r *RedisCache) GetPL(ctx context.Context, pipeliner redis.Pipeliner, key string) (*redis.StringCmd, error) {
	ctx, span := otel.Tracer("datastore").Start(ctx, "RedisCache.GetPL")
	defer span.End()

	if pipeliner == nil {
		c := r.client.Get(ctx, key)
		return c, c.Err()
	}
	c := pipeliner.Get(ctx, key)
	_, err := pipeliner.Exec(ctx)
	if err != nil {
		return nil, err
	}
	return c, err
}

func (r *RedisCache) SetPL(ctx context.Context, pipeliner redis.Pipeliner, key string, value interface{}, expiration time.Duration) error {
	ctx, span := otel.Tracer("datastore").Start(ctx, "RedisCache.SetPL")
	defer span.End()

	if pipeliner == nil {
		return r.client.Set(ctx, key, value, expiration).Err()
	}
	return pipeliner.Set(ctx, key, value, expiration).Err()
}

func (r *RedisCache) HGetPL(ctx context.Context, pipeliner redis.Pipeliner, key, field string) (*redis.StringCmd, error) {
	ctx, span := otel.Tracer("datastore").Start(ctx, "RedisCache.HGetPL")
	defer span.End()

	if pipeliner == nil {
		c := r.client.HGet(ctx, key, field)
		return c, c.Err()
	}

	c := pipeliner.HGet(ctx, key, field)
	_, err := pipeliner.Exec(ctx)
	if err != nil {
		return c, err
	}
	return c, err
}

func (r *RedisCache) HGetAllPL(ctx context.Context, pipeliner redis.Pipeliner, key string) (map[string]string, error) {
	ctx, span := otel.Tracer("datastore").Start(ctx, "RedisCache.HGetAllPL")
	defer span.End()

	if pipeliner == nil {
		return r.client.HGetAll(ctx, key).Result()
	}

	c := pipeliner.HGetAll(ctx, key)
	_, err := pipeliner.Exec(ctx)
	if err != nil && !errors.Is(err, redis.Nil) {
		return nil, err
	}
	value, err := c.Result()
	if err != nil {
		return nil, err
	}
	return value, err
}

func (r *RedisCache) HSetPL(ctx context.Context, pipeliner redis.Pipeliner, key, field string, value interface{}) error {
	ctx, span := otel.Tracer("datastore").Start(ctx, "RedisCache.HSetPL")
	defer span.End()

	if pipeliner == nil {
		return r.client.HSet(ctx, key, field, value).Err()
	}
	return pipeliner.HSet(ctx, key, field, value).Err()
}

func (r *RedisCache) ExpirePL(ctx context.Context, pipeliner redis.Pipeliner, key string, expiration time.Duration) error {
	ctx, span := otel.Tracer("datastore").Start(ctx, "RedisCache.ExpirePL")
	defer span.End()

	if pipeliner == nil {
		return r.client.Expire(ctx, key, expiration).Err()
	}
	return pipeliner.Expire(ctx, key, expiration).Err()
}

func (r *RedisCache) CopyPL(ctx context.Context, pipeliner redis.Pipeliner, src, dest string, db int, replace bool, obj any) error {
	ctx, span := otel.Tracer("datastore").Start(ctx, "RedisCache.CopyPL")
	defer span.End()

	if pipeliner == nil {
		// Have to break this up into a get and set to prevent crossslot errors
		err := r.GetObj(src, obj)
		if err != nil {
			return err
		}
		return r.SetObj(dest, obj, expiryBidCache) // Not generalizable, but works
	}

	c := pipeliner.Copy(ctx, src, dest, db, replace)
	_, err := pipeliner.Exec(ctx)
	if err != nil {
		return err
	}

	wasCopied, copyErr := c.Result()
	if copyErr != nil {
		return copyErr
	} else if wasCopied == 0 {
		return fmt.Errorf("could not copy from %s to %s", src, dest) //nolint:goerr113
	}
	return nil
}

func (r *RedisCache) ExecPL(ctx context.Context, pipeliner redis.Pipeliner) error {
	ctx, span := otel.Tracer("datastore").Start(ctx, "RedisCache.ExecPL")
	defer span.End()

	if pipeliner == nil {
		return nil
	}
	_, err := pipeliner.Exec(ctx)
	return err
}
