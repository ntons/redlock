package distlock

import (
	"context"
	"time"
)

type Lock string

const fakeToken = "0000000000000000000000"

func NewLock(key, token string) Lock {
	if len(token) != 22 {
		token = fakeToken
	}
	return Lock(token + key)
}

func (lock Lock) GetKey() string {
	if len(lock) < 23 {
		return ""
	}
	return string(lock[22:])
}
func (lock Lock) GetToken() string {
	if len(lock) < 23 {
		return fakeToken
	}
	return string(lock[:22])
}

func (lock Lock) TTL(
	ctx context.Context, cli RedisClient) (time.Duration, error) {
	return TTL(ctx, cli, lock)
}
func (lock Lock) Refresh(
	ctx context.Context, cli RedisClient, ttl time.Duration) error {
	return Refresh(ctx, cli, lock, ttl)
}
func (lock Lock) Release(
	ctx context.Context, cli RedisClient) error {
	return Release(ctx, cli, lock)
}
