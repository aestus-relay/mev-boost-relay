package datastore

import (
	"context"
	"errors"
	"math/big"

	"github.com/redis/go-redis/v9"
)

// BuilderBids supports redis.SaveBidAndUpdateTopBid
type BuilderBids struct {
	bidValues map[string]*big.Int
}

func NewBuilderBidsFromRedis(ctx context.Context, r *RedisCache, pipeliner redis.Pipeliner, slot uint64, parentHash, proposerPubkey string) (*BuilderBids, error) {
	keyBidValues := r.keyBlockBuilderLatestBidsValue(slot, parentHash, proposerPubkey)
	c := pipeliner.HGetAll(ctx, keyBidValues)
	_, err := pipeliner.Exec(ctx)
	if err != nil && !errors.Is(err, redis.Nil) {
		return nil, err
	}

	bidValueMap, err := c.Result()
	if err != nil {
		return nil, err
	}
	return NewBuilderBids(bidValueMap), nil
}

func NewBuilderBids(bidValueMap map[string]string) *BuilderBids {
	b := BuilderBids{
		bidValues: make(map[string]*big.Int),
	}
	for builderPubkey, bidValue := range bidValueMap {
		b.bidValues[builderPubkey] = new(big.Int)
		b.bidValues[builderPubkey].SetString(bidValue, 10)
	}
	return &b
}

func (b *BuilderBids) getTopBid() (string, *big.Int) {
	topBidBuilderPubkey := ""
	topBidValue := big.NewInt(0)
	for builderPubkey, bidValue := range b.bidValues {
		if bidValue.Cmp(topBidValue) > 0 {
			topBidValue = bidValue
			topBidBuilderPubkey = builderPubkey
		}
	}
	return topBidBuilderPubkey, topBidValue
}

var (
	// Minified version of topbid.lua
	// The shebang is not standard lua but must be included
	TopBidLuaLibrary = `#!lua name=topbid
local function a(b,c)return'{\n'..'  "slot": '..c..',\n'..'  "getHeaderResponse": '..b..'\n'..'}'end;local function d(e)local f=''local g='0'for h,i in pairs(e)do if tonumber(i)>tonumber(g)then f=h;g=i end end;return f,g end;local function j(k,f,g,l,m,n,o,p)local q=k..f;local r=g;if tonumber(m)>tonumber(g)then q=l;r=m end;redis.call('COPY',q,n,'REPLACE')redis.call('EXPIRE',n,p)redis.call('SET',o,r,'EX',p)return r end;local function s(t,u)redis.setresp(3)local e=redis.call('HGETALL',t[1]).map;local f,g=d(e)local v=redis.call('GET',t[2])or'0'redis.call('PUBLISH','lualog','** Top bid: '..f..' '..g..' Floor bid: '..v)local w=redis.call('GET',t[5])or'0'local x=j(u[1],f,g,t[3],v,t[4],t[5],u[2])if x~=w then local y=redis.call('GET',t[4])local z=a(y,u[4])redis.call('PUBLISH',u[3],z)end;return true end;local function A(t,u)redis.setresp(3)local e=redis.call('HGETALL',t[1]).map;local f,g=d(e)local v=redis.call('GET',t[2])or'0'redis.call('PUBLISH','lualog','** Received bid '..u[2]..' from '..u[1]..' with cancellations '..u[8])redis.call('PUBLISH','lualog','Top bid: '..f..' '..g..' Floor bid: '..v)local B={false,false,false,g,g}if u[8]=='0'and tonumber(u[2])<tonumber(v)then redis.call('PUBLISH','lualog','Bid '..u[2]..' is lower than floor bid '..v)return B end;redis.call('SET',t[3],u[6],'EX',u[9])redis.call('SET',t[4],u[5],'EX',u[9])redis.call('HSET',t[5],u[1],u[4])redis.call('EXPIRE',t[5],u[9])redis.call('HSET',t[1],u[1],u[2])redis.call('EXPIRE',t[1],u[9])redis.call('SET',t[6],u[7],'EX',u[9])B[1]=true;e[u[1]]=u[2]local C,x=d(e)B[4]=x;redis.call('PUBLISH','lualog','Top bid '..g..' -> '..x)if x==g then redis.call('PUBLISH','lualog','Done - Top+floor unchanged')return B end;B[2]=true;B[3]=x==u[2]j(u[3],C,x,t[9],v,t[7],t[8],u[9])local D=a(u[5],u[11])redis.call('PUBLISH',u[10],D)if u[8]~='0'or tonumber(u[2])<tonumber(v)then redis.call('PUBLISH','lualog','Done - Top updated, floor unchanged')return B end;redis.call('COPY',t[4],t[9],'REPLACE')redis.call('EXPIRE',t[9],u[9])redis.call('SET',t[2],u[2],'EX',u[9])redis.call('PUBLISH','lualog','Done - Top+floor updated to '..u[2])return B end;redis.register_function('updateTopBid',s)redis.register_function('saveBidAndUpdateTopBid',A)
`
	LuaFunctionUpdateTopBid = "updateTopBid"
	LuaFunctionSaveBidAndUpdateTopBid = "saveBidAndUpdateTopBid"
)
