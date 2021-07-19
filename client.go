package distlock

import (
	"context"
	"time"
)

type Client struct {
	client RedisClient
}

func New(client RedisClient) *Client {
	return &Client{client: client}
}

func (c *Client) Obtain(ctx context.Context, key string, ttl time.Duration, opts ...Option) (Lock, error) {
	return Obtain(ctx, c.client, key, ttl, opts...)
}

func (c *Client) TTL(ctx context.Context, lock Lock) (time.Duration, error) {
	return TTL(ctx, c.client, lock)
}

func (c *Client) Refresh(ctx context.Context, lock Lock, ttl time.Duration) error {
	return Refresh(ctx, c.client, lock, ttl)
}

func (c *Client) Release(ctx context.Context, lock Lock) error {
	return Release(ctx, c.client, lock)
}

func (c *Client) Ensure(ctx context.Context, lock Lock, ttl time.Duration) error {
	return Ensure(ctx, c.client, lock, ttl)
}
