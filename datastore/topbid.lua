#!lua name=topbid

local function formatTopBidUpdateMsg(topBidJSON, slot)
   return '{\n' ..
      '  "slot": ' .. slot .. ',\n' ..
      '  "getHeaderResponse": ' .. topBidJSON .. '\n' ..
      '}'
end

local function getTopBid(builderBids)
   local topPubkey = ''
   local topValue = '0'
   for pubkey, value in pairs(builderBids) do
      if tonumber(value) > tonumber(topValue) then
         topPubkey = pubkey
         topValue = value
      end
   end
   return topPubkey, topValue
end

-- Corresponding to mev-boost-relay's _updateTopBid, not directly called by redis
-- Returns the new top bid value
local function performTopBidUpdate(keyLatestBidByBuilderPrefix, topPubkey, topValue, keyFloorBid, floorValue, keyCacheGetHeaderResponse, keyTopBidValue, expiryBidCache)
   -- Copy from top bid or floor bid, whichever is greater
   -- Dynamically create the source key, this is not recommended but works in practice outside of clusters:
   -- https://stackoverflow.com/questions/50427304/do-you-have-to-declare-your-keys-in-advance-in-a-redis-script
   local keyBidSource = keyLatestBidByBuilderPrefix .. topPubkey
   local valueSource = topValue
   if tonumber(floorValue) > tonumber(topValue) then
      keyBidSource = keyFloorBid
      valueSource = floorValue
   end

   redis.call('COPY', keyBidSource, keyCacheGetHeaderResponse, 'REPLACE')
   redis.call('EXPIRE', keyCacheGetHeaderResponse, expiryBidCache)

   -- Update the top bid value
   redis.call('SET', keyTopBidValue, valueSource, 'EX', expiryBidCache)

   return valueSource
end

--[[
   Updates the top bid, to be called after bids are modified
   keys: {1: keyBlockBuilderLatestBidsValue,
          2: keyFloorBidValue,
          3: keyFloorBid,
          4: keyCacheGetHeaderResponse,
          5: keyTopBidValue}
   args: {1: keyLatestBidByBuilderPrefix,
          2: expiryBidCache,
          3, topBidUpdateChannel,
          4: slot}
]]
local function updateTopBid(keys, args)
   redis.setresp(3)
   local builderBids = redis.call('HGETALL', keys[1]).map
   local topPubkey, topValue = getTopBid(builderBids)
   local floorBidValue = redis.call('GET', keys[2]) or '0'
   redis.call('PUBLISH', 'lualog', '** Top bid: ' .. topPubkey .. ' ' .. topValue .. ' Floor bid: ' .. floorBidValue)

   -- Update the top bid, note if it changed
   local prevTopValue = redis.call('GET', keys[5]) or '0'
   local newTopValue = performTopBidUpdate(args[1], topPubkey, topValue, keys[3], floorBidValue, keys[4], keys[5], args[2])

   -- Publish the new top bid if it changed
   if newTopValue ~= prevTopValue then
      local topGetHeaderResp = redis.call('GET', keyCacheGetHeaderResponse)
      local topBidUpdate = formatTopBidUpdateMsg(topGetHeaderResp, args[4])
      redis.call('PUBLISH', args[3], topBidUpdate)
   end
   return
end

--[[
 keys: {1: keyBlockBuilderLatestBidsValue,
        2: keyFloorBidValue,
        3: keyPayloadContents,
        4: keyLatestBidByBuilder,
        5: keyBlockBuilderLatestBidsTime,
        6: keyCacheBidTrace,
        7: keyCacheGetHeaderResponse,
        8: keyTopBidValue,
        9: keyFloorBid}
 args: {1: builderPubkey,
        2: value,
        3: keyLatestBidByBuilderPrefix,
        4: receivedAtTime,
        5: headerResp,
        6: getPayloadResponse,
        7: bidTrace,
        8: isCancellationEnabled,
        9: expiryBidCache,
        10, topBidUpdateChannel,
        11: slot}
 returns: {
        1: WasBidSaved,
        2: WasTopBidUpdated,
        3: IsNewTopBid,
        4: TopBidValue,
        5: PrevTopBidValue}
]]
local function saveBidAndUpdateTopBid(keys, args)
   redis.setresp(3)
   local builderBids = redis.call('HGETALL', keys[1]).map
   local topPubkey, topValue = getTopBid(builderBids)
   local floorBidValue = redis.call('GET', keys[2]) or '0'
   redis.call('PUBLISH', 'lualog', '** Received bid ' .. args[2] .. ' from ' .. args[1] .. ' with cancellations ' .. args[8])
   redis.call('PUBLISH', 'lualog', 'Top bid: ' .. topPubkey .. ' ' .. topValue .. ' Floor bid: ' .. floorBidValue)

   local state = {false, false, false, topValue, topValue}

   -- Abort if non-cancellation bid is lower than floor bid
   if args[8] == '0' and tonumber(args[2]) < tonumber(floorBidValue) then
      redis.call('PUBLISH', 'lualog', 'Bid ' .. args[2] .. ' is lower than floor bid ' .. floorBidValue)
      return {err = 'Bid is lower than floor bid'}
   end

   -- Save the execution payload
   redis.call('SET', keys[3], args[6], 'EX', args[9])

   -- Save the latest bid for this builder
   -- Bid / headerResp
   redis.call('SET', keys[4], args[5], 'EX', args[9])
   -- Time
   redis.call('HSET', keys[5], args[1], args[4])
   redis.call('EXPIRE', keys[5], args[9])
   -- Value
   redis.call('HSET', keys[1], args[1], args[2])
   redis.call('EXPIRE', keys[1], args[9])

   -- Save the bid trace
   redis.call('SET', keys[6], args[7], 'EX', args[9])
   state[1] = true

   -- Calculate the new top bid, abort if unchanged
   builderBids[args[1]] = args[2]
   local newTopPubkey, newTopValue = getTopBid(builderBids)
   state[4] = newTopValue
   redis.call('PUBLISH', 'lualog', 'Top bid ' .. topValue .. ' -> ' .. newTopValue)
   if newTopValue == topValue then
      redis.call('PUBLISH', 'lualog', 'Done - Top+floor unchanged')
      return state
   end

   state[2] = true
   state[3] = newTopValue == args[2]

   -- Update the global top bid
   performTopBidUpdate(args[3], newTopPubkey, newTopValue, keys[9], floorBidValue, keys[7], keys[8], args[9])

   -- Publish a top bid update
   local topBidUpdateMsg = formatTopBidUpdateMsg(args[5], args[11])
   redis.call('PUBLISH', args[10], topBidUpdateMsg)

   -- A non-cancellable bid above the floor should set a new floor
   -- i.e. if the bid is cancellable or below the floor, stop here
   if args[8] ~= '0' or tonumber(args[2]) < tonumber(floorBidValue) then
      redis.call('PUBLISH', 'lualog', 'Done - Top updated, floor unchanged')
      return state
   end
   
   -- Bid
   redis.call('COPY', keys[4], keys[9], 'REPLACE')
   redis.call('EXPIRE', keys[9], args[9])
   -- Value
   redis.call('SET', keys[2], args[2], 'EX', args[9])

   redis.call('PUBLISH', 'lualog', 'Done - Top+floor updated to ' .. args[2])
   return state
end

redis.register_function('updateTopBid', updateTopBid)
redis.register_function('saveBidAndUpdateTopBid', saveBidAndUpdateTopBid)
