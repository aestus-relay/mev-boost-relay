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
local function a(b,c,d)return'{'..'  "slot": '..c..','..'  "timestamp": '..d..','..'  "getHeaderResponse": '..b..'}'end;local function e()local f=redis.call('TIME')return f[1]*1000+math.floor(f[2]/1000)end;local function g(h)local i=''local j='0'for k,l in pairs(h)do if tonumber(l)>tonumber(j)then i=k;j=l end end;return i,j end;local function m(n,i,j,o,p,q,r,s)local t=n..i;local u=j;if tonumber(p)>tonumber(j)then t=o;u=p end;redis.call('COPY',t,q,'REPLACE')redis.call('EXPIRE',q,s)redis.call('SET',r,u,'EX',s)return u end;local function v(w,x)redis.setresp(3)local h=redis.call('HGETALL',w[1]).map;local i,j=g(h)local y=redis.call('GET',w[2])or'0'redis.call('PUBLISH','lualog','** Top bid: '..i..' '..j..' Floor bid: '..y)local z=redis.call('GET',w[5])or'0'local A=m(x[1],i,j,w[3],y,w[4],w[5],x[2])if A~=z then local B=redis.call('GET',w[4])local C=a(B,x[4],e())redis.call('PUBLISH',x[3],C)end;return true end;local function D(w,x)redis.setresp(3)local h=redis.call('HGETALL',w[1]).map;local i,j=g(h)local y=redis.call('GET',w[2])or'0'redis.call('PUBLISH','lualog','** Received bid '..x[2]..' from '..x[1]..' with cancellations '..x[6])redis.call('PUBLISH','lualog','Top bid: '..i..' '..j..' Floor bid: '..y)local E={false,false,false,j,j}if x[6]=='0'and tonumber(x[2])<tonumber(y)then redis.call('PUBLISH','lualog','Bid '..x[2]..' is lower than floor bid '..y)return E end;redis.call('SET',w[3],x[5],'EX',x[7])redis.call('HSET',w[4],x[1],x[4])redis.call('EXPIRE',w[4],x[7])redis.call('HSET',w[1],x[1],x[2])redis.call('EXPIRE',w[1],x[7])E[1]=true;h[x[1]]=x[2]local F,A=g(h)E[4]=A;redis.call('PUBLISH','lualog','Top bid '..j..' -> '..A)if A==j then redis.call('PUBLISH','lualog','Done - Top+floor unchanged')return E end;E[2]=true;E[3]=A==x[2]m(x[3],F,A,w[7],y,w[5],w[6],x[7])local G=a(x[5],x[9],e())redis.call('PUBLISH',x[8],G)if x[6]~='0'or tonumber(x[2])<tonumber(y)then redis.call('PUBLISH','lualog','Done - Top updated, floor unchanged')return E end;redis.call('COPY',w[3],w[7],'REPLACE')redis.call('EXPIRE',w[7],x[7])redis.call('SET',w[2],x[2],'EX',x[7])redis.call('PUBLISH','lualog','Done - Top+floor updated to '..x[2])return E end;redis.register_function('updateTopBid',v)redis.register_function('processBidAndUpdateTopBid',D)
`
	LuaFunctionUpdateTopBid = "updateTopBid"
	LuaFunctionProcessBidAndUpdateTopBid = "processBidAndUpdateTopBid"
)
