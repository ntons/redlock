package distlock

import (
	"context"
	"time"
)

// extend lock
func newLock(key, value string) *Lock {
	return &Lock{Key: key, Value: value}
}

// Token returns the token value set by the lock.
func (l *Lock) Token() string {
	return l.Value[:22]
}

// Metadata returns the metadata of the lock.
func (l *Lock) Metadata() string {
	return l.Value[22:]
}

// EasyLock bind client to itself for the ease to use
type EasyLock struct {
	*Lock
	client RedisClient
}

// bind client to lock, an extended lock returned
func Bind(lock *Lock, client RedisClient) *EasyLock {
	return &EasyLock{Lock: lock, client: client}
}
func (l *EasyLock) TTL(ctx context.Context) (time.Duration, error) {
	return TTL(ctx, l.client, l.Lock)
}
func (l *EasyLock) Refresh(ctx context.Context, ttl time.Duration) error {
	return Refresh(ctx, l.client, l.Lock, ttl)
}
func (l *EasyLock) Release(ctx context.Context) error {
	return Release(ctx, l.client, l.Lock)
}
