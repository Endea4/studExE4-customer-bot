package main

import (
	"context"
	"encoding/json"
	"fmt"
	"time"

	"github.com/redis/go-redis/v9"
)

const (
	redisKeyState   = "studex:bot:state"
	redisKeyPartner = "studex:bot:partner"
	redisKeyLastMsg = "studex:bot:lastmsg"
	stateTTL        = 24 * time.Hour
)

var rdb *redis.Client

func initRedisClient(addr string) {
	rdb = redis.NewClient(&redis.Options{Addr: addr})
	if err := rdb.Ping(context.Background()).Err(); err != nil {
		fmt.Printf("Warning: Redis state connect failed: %v\n", err)
		rdb = nil
	} else {
		fmt.Printf("Redis state store connected: %s\n", addr)
	}
}

func redisStateKey(phone string) string     { return fmt.Sprintf("%s:%s", redisKeyState, phone) }
func redisPartnerKey(phone string) string   { return fmt.Sprintf("%s:%s", redisKeyPartner, phone) }
func redisLastMsgKey(phone string) string   { return fmt.Sprintf("%s:%s", redisKeyLastMsg, phone) }

func saveUserState(phone string, state *UserState) {
	if state == nil {
		return
	}
	if userStates == nil {
		userStates = make(map[string]*UserState)
	}
	userStates[phone] = state
	if rdb == nil {
		return
	}
	data, err := json.Marshal(state)
	if err != nil {
		fmt.Printf("[REDIS] marshal state failed: phone=%s err=%v\n", phone, err)
		return
	}
	if err := rdb.Set(context.Background(), redisStateKey(phone), data, stateTTL).Err(); err != nil {
		fmt.Printf("[REDIS] save state failed: phone=%s err=%v\n", phone, err)
	}
}

func loadUserState(phone string) (*UserState, bool) {
	if userStates == nil {
		userStates = make(map[string]*UserState)
	}
	if state, ok := userStates[phone]; ok {
		return state, true
	}
	if rdb == nil {
		return nil, false
	}
	data, err := rdb.Get(context.Background(), redisStateKey(phone)).Bytes()
	if err != nil {
		return nil, false
	}
	var state UserState
	if json.Unmarshal(data, &state) != nil {
		return nil, false
	}
	userStates[phone] = &state
	return &state, true
}

func deleteUserState(phone string) {
	if userStates != nil {
		delete(userStates, phone)
	}
	if rdb != nil {
		rdb.Del(context.Background(), redisStateKey(phone))
	}
}

func savePartner(phone, partner string) {
	if activeTripPartner == nil {
		activeTripPartner = make(map[string]string)
	}
	activeTripPartner[phone] = partner
	if rdb == nil {
		return
	}
	if err := rdb.Set(context.Background(), redisPartnerKey(phone), partner, stateTTL).Err(); err != nil {
		fmt.Printf("[REDIS] save partner failed: phone=%s err=%v\n", phone, err)
	}
}

func loadPartner(phone string) (string, bool) {
	if activeTripPartner == nil {
		activeTripPartner = make(map[string]string)
	}
	if partner, ok := activeTripPartner[phone]; ok {
		return partner, true
	}
	if rdb == nil {
		return "", false
	}
	partner, err := rdb.Get(context.Background(), redisPartnerKey(phone)).Result()
	if err != nil {
		return "", false
	}
	activeTripPartner[phone] = partner
	return partner, true
}

func deletePartner(phone string) {
	if activeTripPartner != nil {
		delete(activeTripPartner, phone)
	}
	if rdb != nil {
		rdb.Del(context.Background(), redisPartnerKey(phone))
	}
}

func deletePartnerReverse(phone string) {
	if activeTripPartner != nil {
		for k, v := range activeTripPartner {
			if v == phone {
				delete(activeTripPartner, k)
			}
		}
	}
	if rdb == nil {
		return
	}
	iter := rdb.Scan(context.Background(), 0, redisKeyPartner+":*", 0).Iterator()
	for iter.Next(context.Background()) {
		val, err := rdb.Get(context.Background(), iter.Val()).Result()
		if err == nil && val == phone {
			rdb.Del(context.Background(), iter.Val())
		}
	}
}

func saveLastMsg(phone string, t time.Time) {
	if lastCustomerMessage == nil {
		lastCustomerMessage = make(map[string]time.Time)
	}
	lastCustomerMessage[phone] = t
	if rdb != nil {
		rdb.Set(context.Background(), redisLastMsgKey(phone), t.UnixMilli(), stateTTL)
	}
}

func loadLastMsg(phone string) (time.Time, bool) {
	if lastCustomerMessage == nil {
		lastCustomerMessage = make(map[string]time.Time)
	}
	if t, ok := lastCustomerMessage[phone]; ok {
		return t, true
	}
	if rdb == nil {
		return time.Time{}, false
	}
	ms, err := rdb.Get(context.Background(), redisLastMsgKey(phone)).Int64()
	if err != nil {
		return time.Time{}, false
	}
	t := time.UnixMilli(ms)
	lastCustomerMessage[phone] = t
	return t, true
}

func deleteLastMsg(phone string) {
	if lastCustomerMessage != nil {
		delete(lastCustomerMessage, phone)
	}
	if rdb != nil {
		rdb.Del(context.Background(), redisLastMsgKey(phone))
	}
}

func resetAllState() {
	if userStates != nil {
		userStates = make(map[string]*UserState)
	}
	if activeTripPartner != nil {
		activeTripPartner = make(map[string]string)
	}
	if lastCustomerMessage != nil {
		lastCustomerMessage = make(map[string]time.Time)
	}
	if rdb == nil {
		return
	}
	ctx := context.Background()
	for _, prefix := range []string{redisKeyState, redisKeyPartner, redisKeyLastMsg} {
		iter := rdb.Scan(ctx, 0, prefix+":*", 0).Iterator()
		for iter.Next(ctx) {
			rdb.Del(ctx, iter.Val())
		}
	}
}
