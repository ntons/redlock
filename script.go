package distlock

import (
	"context"
	"crypto/sha1"
	"encoding/hex"
	"io"
	"strings"
	"sync"
	"time"

	"github.com/go-redis/redis/v8"
)

// Client is a minimal client interface.
type RedisClient interface {
	SetNX(ctx context.Context, key string, value interface{}, expiration time.Duration) *redis.BoolCmd
	EvalSha(ctx context.Context, sha1 string, keys []string, args ...interface{}) *redis.Cmd
	ScriptLoad(ctx context.Context, script string) *redis.StringCmd
}

type script struct {
	src, hash string
	// mutex for loading
	mu sync.Mutex
}

func newScript(src string) *script {
	h := sha1.New()
	_, _ = io.WriteString(h, src)
	return &script{src: src, hash: hex.EncodeToString(h.Sum(nil))}
}

func isNoScript(err error) bool {
	if err == nil {
		return false
	}
	return err != nil && strings.HasPrefix(err.Error(), "NOSCRIPT ")
}

func (script *script) Run(ctx context.Context, cli RedisClient, keys []string, args ...interface{}) (r *redis.Cmd) {
	if r = cli.EvalSha(ctx, script.hash, keys, args...); !isNoScript(r.Err()) {
		return
	}
	script.mu.Lock()
	if r = cli.EvalSha(ctx, script.hash, keys, args...); !isNoScript(r.Err()) {
		script.mu.Unlock()
		return
	}
	if err := cli.ScriptLoad(ctx, script.src).Err(); err != nil {
		script.mu.Unlock()
		r = redis.NewCmd(ctx)
		r.SetErr(err)
		return
	}
	script.mu.Unlock()
	return cli.EvalSha(ctx, script.hash, keys, args...)
}
