#!lua name=topbid

local function formatTopBidUpdateMsg(topBidJSON, slot, timestamp, builderPubkey)
   return '{' ..
      '  "slot": ' .. slot .. ',' ..
      '  "timestamp": ' .. timestamp .. ',' ..
      '  "builderPubkey": "' .. (builderPubkey) .. '",' ..
      '  "getHeaderResponse": ' .. topBidJSON ..
      '}'
end

local function getCurrentTimeMs()
   local time = redis.call('TIME')
   return time[1] * 1000 + math.floor(time[2] / 1000)
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
      local topGetHeaderResp = redis.call('GET', keys[4])
      local topBidUpdate = formatTopBidUpdateMsg(topGetHeaderResp, args[4], getCurrentTimeMs(), topPubkey)
      redis.call('PUBLISH', args[3], topBidUpdate)
   end
   return true
end

--[[
 keys: {1: keyBlockBuilderLatestBidsValue,
        2: keyFloorBidValue,
        3: keyLatestBidByBuilder,
        4: keyBlockBuilderLatestBidsTime,
        5: keyCacheGetHeaderResponse,
        6: keyTopBidValue,
        7: keyFloorBid}
 args: {1: builderPubkey,
        2: value,
        3: keyLatestBidByBuilderPrefix,
        4: receivedAtTime,
        5: headerResp,
        6: isCancellationEnabled,
        7: expiryBidCache,
        8, topBidUpdateChannel,
        9: slot}
 returns: {
        1: WasBidSaved,
        2: WasTopBidUpdated,
        3: IsNewTopBid,
        4: TopBidValue,
        5: PrevTopBidValue}
]]
local function processBidAndUpdateTopBid(keys, args)
   redis.setresp(3)
   local builderBids = redis.call('HGETALL', keys[1]).map
   local topPubkey, topValue = getTopBid(builderBids)
   local floorBidValue = redis.call('GET', keys[2]) or '0'
   redis.call('PUBLISH', 'lualog', '** Received bid ' .. args[2] .. ' from ' .. args[1] .. ' with cancellations ' .. args[6])
   redis.call('PUBLISH', 'lualog', 'Top bid: ' .. topPubkey .. ' ' .. topValue .. ' Floor bid: ' .. floorBidValue)

   local state = {false, false, false, topValue, topValue}

   -- Abort if non-cancellation bid is lower than floor bid
   if args[6] == '0' and tonumber(args[2]) < tonumber(floorBidValue) then
      redis.call('PUBLISH', 'lualog', 'Bid ' .. args[2] .. ' is lower than floor bid ' .. floorBidValue)
      return state
   end

   -- Save the latest bid for this builder
   -- Bid / headerResp
   redis.call('SET', keys[3], args[5], 'EX', args[7])
   -- Time
   redis.call('HSET', keys[4], args[1], args[4])
   redis.call('EXPIRE', keys[4], args[7])
   -- Value
   redis.call('HSET', keys[1], args[1], args[2])
   redis.call('EXPIRE', keys[1], args[7])

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
   performTopBidUpdate(args[3], newTopPubkey, newTopValue, keys[7], floorBidValue, keys[5], keys[6], args[7])

   -- Publish a top bid update
   local topBidUpdateMsg = formatTopBidUpdateMsg(args[5], args[9], getCurrentTimeMs(), newTopPubkey)
   redis.call('PUBLISH', args[8], topBidUpdateMsg)

   -- A non-cancellable bid above the floor should set a new floor
   -- i.e. if the bid is cancellable or below the floor, stop here
   if args[6] ~= '0' or tonumber(args[2]) < tonumber(floorBidValue) then
      redis.call('PUBLISH', 'lualog', 'Done - Top updated, floor unchanged')
      return state
   end
   
   -- Bid
   redis.call('COPY', keys[3], keys[7], 'REPLACE')
   redis.call('EXPIRE', keys[7], args[7])
   -- Value
   redis.call('SET', keys[2], args[2], 'EX', args[7])

   redis.call('PUBLISH', 'lualog', 'Done - Top+floor updated to ' .. args[2])
   return state
end

redis.register_function('updateTopBid', updateTopBid)
redis.register_function('processBidAndUpdateTopBid', processBidAndUpdateTopBid)
