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
	luaRefresh = newScript(`if redis.call("get", KEYS[1]) == ARGV[1] then return redis.call("pexpire", KEYS[1], ARGV[2]) else return 0 end`)
	luaRelease = newScript(`if redis.call("get", KEYS[1]) == ARGV[1] then return redis.call("del", KEYS[1]) else return 0 end`)
	luaPTTL    = newScript(`if redis.call("get", KEYS[1]) == ARGV[1] then return redis.call("pttl", KEYS[1]) else return -3 end`)
	luaEnsure  = newScript(`if redis.call("get", KEYS[1]) == ARGV[1] then if redis.call("pttl", KEYS[1]) < tonumber(ARGV[2]) then return redis.call("pexpire", KEYS[1], ARGV[2]) else return 1 end else return 0 end`)
)

var (
	// ErrNotObtained is returned when a lock cannot be obtained.
	ErrNotObtained = errors.New("distlock: not obtained")
	// ErrLockNotHeld is returned when trying to release an inactive lock.
	ErrLockNotHeld = errors.New("distlock: lock not held")
)

// Obtain tries to obtain a new lock using a key with the given TTL.
// May return ErrNotObtained if not successful.
func Obtain(ctx context.Context, cli RedisClient, key string, ttl time.Duration, opts ...Option) (Lock, error) {
	var o options
	for _, opt := range opts {
		opt.apply(&o)
	}

	// generate a random token
	var buf [16]byte
	if _, err := io.ReadFull(rand.Reader, buf[:]); err != nil {
		panic(err)
	}
	token := base64.RawURLEncoding.EncodeToString(buf[:])

	ctx, cancel := context.WithTimeout(ctx, ttl)
	defer cancel()

	for retry, timer := o.getRetryStrategy(), (*time.Timer)(nil); ; {
		ok, err := cli.SetNX(ctx, key, token, ttl).Result()
		if err != nil {
			return "", err
		} else if ok {
			return NewLock(key, token), nil
		}

		backoff := retry.NextBackoff()
		if backoff < 1 {
			return "", ErrNotObtained
		}

		if timer == nil {
			timer = time.NewTimer(backoff)
			defer timer.Stop()
		} else {
			timer.Reset(backoff)
		}

		select {
		case <-ctx.Done():
			return "", ErrNotObtained
		case <-timer.C:
		}
	}
}

// TTL returns the remaining time-to-live. Returns 0 if the lock has expired.
func TTL(ctx context.Context, cli RedisClient, lock Lock) (time.Duration, error) {
	res, err := luaPTTL.Run(ctx, cli, []string{lock.GetKey()}, lock.GetToken()).Result()
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
func Refresh(ctx context.Context, cli RedisClient, lock Lock, ttl time.Duration) error {
	ctx, cancel := context.WithTimeout(ctx, ttl)
	defer cancel()

	ttlVal := strconv.FormatInt(int64(ttl/time.Millisecond), 10)
	status, err := luaRefresh.Run(
		ctx, cli, []string{lock.GetKey()}, lock.GetToken(), ttlVal).Result()
	if err != nil {
		return err
	} else if status == int64(1) {
		return nil
	}
	return ErrNotObtained
}

// Release manually releases the lock.
// May return ErrLockNotHeld.
func Release(
	ctx context.Context, cli RedisClient, lock Lock) error {
	res, err := luaRelease.Run(
		ctx, cli, []string{lock.GetKey()}, lock.GetToken()).Result()
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

// Ensure ttl greater than
func Ensure(ctx context.Context, cli RedisClient, lock Lock, ttl time.Duration) error {
	ctx, cancel := context.WithTimeout(ctx, ttl)
	defer cancel()

	ttlVal := strconv.FormatInt(int64(ttl/time.Millisecond), 10)
	status, err := luaEnsure.Run(
		ctx, cli, []string{lock.GetKey()}, lock.GetToken(), ttlVal).Result()
	if err != nil {
		return err
	} else if status == int64(1) {
		return nil
	}
	return ErrNotObtained
}
