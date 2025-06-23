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
local function a(b,c,d,e)return'{'..'  "slot": '..c..','..'  "timestamp": '..d..','..'  "builderPubkey": "'..e..'",'..'  "getHeaderResponse": '..b..'}'end;local function f()local g=redis.call('TIME')return g[1]*1000+math.floor(g[2]/1000)end;local function h(i)local j=''local k='0'for l,m in pairs(i)do if tonumber(m)>tonumber(k)then j=l;k=m end end;return j,k end;local function n(o,j,k,p,q,r,s,t)local u=o..j;local v=k;if tonumber(q)>tonumber(k)then u=p;v=q end;redis.call('COPY',u,r,'REPLACE')redis.call('EXPIRE',r,t)redis.call('SET',s,v,'EX',t)return v end;local function w(x,y)redis.setresp(3)local i=redis.call('HGETALL',x[1]).map;local j,k=h(i)local z=redis.call('GET',x[2])or'0'redis.call('PUBLISH','lualog','** Top bid: '..j..' '..k..' Floor bid: '..z)local A=redis.call('GET',x[5])or'0'local B=n(y[1],j,k,x[3],z,x[4],x[5],y[2])if B~=A then local C=redis.call('GET',x[4])local D=a(C,y[4],f(),j)redis.call('PUBLISH',y[3],D)end;return true end;local function E(x,y)redis.setresp(3)local i=redis.call('HGETALL',x[1]).map;local j,k=h(i)local z=redis.call('GET',x[2])or'0'redis.call('PUBLISH','lualog','** Received bid '..y[2]..' from '..y[1]..' with cancellations '..y[6])redis.call('PUBLISH','lualog','Top bid: '..j..' '..k..' Floor bid: '..z)local F={false,false,false,k,k}if y[6]=='0'and tonumber(y[2])<tonumber(z)then redis.call('PUBLISH','lualog','Bid '..y[2]..' is lower than floor bid '..z)return F end;redis.call('SET',x[3],y[5],'EX',y[7])redis.call('HSET',x[4],y[1],y[4])redis.call('EXPIRE',x[4],y[7])redis.call('HSET',x[1],y[1],y[2])redis.call('EXPIRE',x[1],y[7])F[1]=true;i[y[1]]=y[2]local G,B=h(i)F[4]=B;redis.call('PUBLISH','lualog','Top bid '..k..' -> '..B)if B==k then redis.call('PUBLISH','lualog','Done - Top+floor unchanged')return F end;F[2]=true;F[3]=B==y[2]n(y[3],G,B,x[7],z,x[5],x[6],y[7])local H=a(y[5],y[9],f(),G)redis.call('PUBLISH',y[8],H)if y[6]~='0'or tonumber(y[2])<tonumber(z)then redis.call('PUBLISH','lualog','Done - Top updated, floor unchanged')return F end;redis.call('COPY',x[3],x[7],'REPLACE')redis.call('EXPIRE',x[7],y[7])redis.call('SET',x[2],y[2],'EX',y[7])redis.call('PUBLISH','lualog','Done - Top+floor updated to '..y[2])return F end;redis.register_function('updateTopBid',w)redis.register_function('processBidAndUpdateTopBid',E)
`
	LuaFunctionUpdateTopBid = "updateTopBid"
	LuaFunctionProcessBidAndUpdateTopBid = "processBidAndUpdateTopBid"
)
