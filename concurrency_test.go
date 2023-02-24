package distlock

import (
	"context"
	"fmt"
	"math/rand"
	"sync"
	"testing"
	"time"

	"github.com/go-redis/redis/v8"
)

func TestCon(t *testing.T) {
	const key = "__ntons_distlock_go_unit_test__"

	var ctx = context.Background()

	{
		cli := redis.NewClient(&redis.Options{
			Network: "tcp",
			Addr:    "127.0.0.1:6379", DB: 9,
		})

		fmt.Println("del:", cli.Del(ctx, key).Err())
	}

	{
		x := LimitRetry(
			// 32 + 32 + 32 + 32 + 32 + 64 + 128 + 256 + 512 + 512 = 1632
			ExponentialBackoff(
				32*time.Millisecond,
				512*time.Millisecond), 10)
		for t := x.NextBackoff(); t > 0; t = x.NextBackoff() {
			fmt.Println(t)
		}
	}

	cli := New(redis.NewClient(&redis.Options{
		Network: "tcp",
		Addr:    "127.0.0.1:6379", DB: 9,
	}))

	var wg sync.WaitGroup

	for i := 0; i < 10; i++ {
		wg.Add(1)
		go func(idx int) {
			defer wg.Done()

			<-time.After(time.Duration(rand.Intn(100)) * time.Millisecond)

			t := time.Now()

			lock, err := cli.Obtain(ctx, key, 10*time.Second,
				WithRetryStrategy(LimitRetry(
					// 32 + 32 + 32 + 32 + 32 + 64 + 128 + 256 + 512 + 512 = 1632
					ExponentialBackoff(
						32*time.Millisecond,
						512*time.Millisecond), 10)))
			if err != nil {
				fmt.Printf("%d: failed to lock, %v, %v\n", idx, err, time.Since(t))
				return
			}

			fmt.Printf("%d: locked\n", idx)

			<-time.After(time.Duration(rand.Intn(1000)) * time.Millisecond)

			fmt.Printf("%d: unlocking\n", idx)

			if err := cli.Release(ctx, lock); err != nil {
				fmt.Printf("%d: failed to unlock, %v\n", idx, err)
				return
			}
		}(i)
	}

	wg.Wait()
}
