package distlock

import (
	"context"
	"crypto/sha1"
	"encoding/hex"
	"io"
	"math/rand"
	"strings"
	"sync/atomic"

	"github.com/go-redis/redis/v8"
)

var (
	// scripts to load
	scripts []*script
	// try evalsha probability
	tryEvalShaProb int32
)

func SetTryEvalShaProb(v int32) {
	atomic.StoreInt32(&tryEvalShaProb, v)
}
func shouldTryEvalSha() bool {
	if v := atomic.LoadInt32(&tryEvalShaProb); v > 0 {
		return rand.Int31n(v) == 0
	} else {
		return rand.Int31n(100) == 0
	}
}

// load all scripts to redis
// ReMon can work without invoking LoadScripts or LoadScripts failed
func LoadScripts(ctx context.Context, cli RedisClient) (err error) {
	for _, script := range scripts {
		if _, err = script.Load(ctx, cli).Result(); err != nil {
			return
		}
	}
	return
}

// Script is almost same to redis.Script
// But evalsha trying strategy in run method
type script struct {
	src    string
	hash   string
	loaded bool // is script loaded
}

func newScript(src string) *script {
	h := sha1.New()
	_, _ = io.WriteString(h, src)
	script := &script{
		src:    src,
		hash:   hex.EncodeToString(h.Sum(nil)),
		loaded: true,
	}
	scripts = append(scripts, script)
	return script
}

func (script *script) Hash() string {
	return script.hash
}

func (script *script) Load(
	ctx context.Context, cli RedisClient) *redis.StringCmd {
	return cli.ScriptLoad(ctx, script.src)
}

func (script *script) Eval(
	ctx context.Context, cli RedisClient,
	keys []string, args ...interface{}) *redis.Cmd {
	return cli.Eval(ctx, script.src, keys, args...)
}

func (script *script) EvalSha(
	ctx context.Context, cli RedisClient,
	keys []string, args ...interface{}) *redis.Cmd {
	return cli.EvalSha(ctx, script.hash, keys, args...)
}

func (script *script) Run(
	ctx context.Context, cli RedisClient,
	keys []string, args ...interface{}) *redis.Cmd {
	if !script.loaded {
		script.loaded = shouldTryEvalSha()
	}
	if script.loaded {
		r := script.EvalSha(ctx, cli, keys, args...)
		if err := r.Err(); err == nil ||
			!strings.HasPrefix(err.Error(), "NOSCRIPT ") {
			return r
		}
		script.loaded = false
	}
	return script.Eval(ctx, cli, keys, args...)
}
