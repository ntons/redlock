package distlock

import (
	"context"
	"crypto/rand"
	"encoding/base64"
	"errors"
	"io"
	"strconv"
	"time"

	"github.com/go-redis/redis/v8"
)

var (
	luaRefresh = redis.NewScript(`if redis.call("get", KEYS[1]) == ARGV[1] then return redis.call("pexpire", KEYS[1], ARGV[2]) else return 0 end`)
	luaRelease = redis.NewScript(`if redis.call("get", KEYS[1]) == ARGV[1] then return redis.call("del", KEYS[1]) else return 0 end`)
	luaPTTL    = redis.NewScript(`if redis.call("get", KEYS[1]) == ARGV[1] then return redis.call("pttl", KEYS[1]) else return -3 end`)
)

var (
	// ErrNotObtained is returned when a lock cannot be obtained.
	ErrNotObtained = errors.New("distlock: not obtained")

	// ErrLockNotHeld is returned when trying to release an inactive lock.
	ErrLockNotHeld = errors.New("distlock: lock not held")
)

// Client is a minimal client interface.
type RedisClient interface {
	SetNX(ctx context.Context, key string, value interface{}, expiration time.Duration) *redis.BoolCmd
	Eval(ctx context.Context, script string, keys []string, args ...interface{}) *redis.Cmd
	EvalSha(ctx context.Context, sha1 string, keys []string, args ...interface{}) *redis.Cmd
	ScriptExists(ctx context.Context, scripts ...string) *redis.BoolSliceCmd
	ScriptLoad(ctx context.Context, script string) *redis.StringCmd
}

// Obtain tries to obtain a new lock using a key with the given TTL.
// May return ErrNotObtained if not successful.
func Obtain(ctx context.Context, client RedisClient, key string, ttl time.Duration, opts ...Option) (*Lock, error) {
	var o options
	for _, opt := range opts {
		opt.apply(&o)
	}
	// Create a random token
	token, err := randomToken()
	if err != nil {
		return nil, err
	}

	value := token + o.getMetadata()
	retry := o.getRetryStrategy()

	var cancel context.CancelFunc = func() {}
	if _, ok := ctx.Deadline(); !ok {
		ctx, cancel = context.WithDeadline(ctx, time.Now().Add(ttl))
	}
	defer cancel()

	var timer *time.Timer
	for {
		ok, err := obtain(ctx, client, key, value, ttl)
		if err != nil {
			return nil, err
		} else if ok {
			return newLock(key, value), nil
		}

		backoff := retry.NextBackoff()
		if backoff < 1 {
			return nil, ErrNotObtained
		}

		if timer == nil {
			timer = time.NewTimer(backoff)
			defer timer.Stop()
		} else {
			timer.Reset(backoff)
		}

		select {
		case <-ctx.Done():
			return nil, ErrNotObtained
		case <-timer.C:
		}
	}
}

// TTL returns the remaining time-to-live. Returns 0 if the lock has expired.
func TTL(ctx context.Context, client RedisClient, lock *Lock) (time.Duration, error) {
	res, err := luaPTTL.Run(ctx, client, []string{lock.Key}, lock.Value).Result()
	if err == redis.Nil {
		return 0, nil
	} else if err != nil {
		return 0, err
	}

	if num := res.(int64); num > 0 {
		return time.Duration(num) * time.Millisecond, nil
	}
	return 0, nil
}

// Refresh extends the lock with a new TTL.
// May return ErrNotObtained if refresh is unsuccessful.
func Refresh(ctx context.Context, client RedisClient, lock *Lock, ttl time.Duration) error {
	ttlVal := strconv.FormatInt(int64(ttl/time.Millisecond), 10)
	status, err := luaRefresh.Run(ctx, client, []string{lock.Key}, lock.Value, ttlVal).Result()
	if err != nil {
		return err
	} else if status == int64(1) {
		return nil
	}
	return ErrNotObtained
}

// Release manually releases the lock.
// May return ErrLockNotHeld.
func Release(ctx context.Context, client RedisClient, lock *Lock) error {
	res, err := luaRelease.Run(ctx, client, []string{lock.Key}, lock.Value).Result()
	if err == redis.Nil {
		return ErrLockNotHeld
	} else if err != nil {
		return err
	}

	if i, ok := res.(int64); !ok || i != 1 {
		return ErrLockNotHeld
	}
	return nil
}

func obtain(ctx context.Context, client RedisClient, key, value string, ttl time.Duration) (bool, error) {
	return client.SetNX(ctx, key, value, ttl).Result()
}

func randomToken() (string, error) {
	tmp := make([]byte, 16)
	if _, err := io.ReadFull(rand.Reader, tmp); err != nil {
		return "", err
	}
	return base64.RawURLEncoding.EncodeToString(tmp), nil
}
