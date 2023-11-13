package redlock_test

import (
	"context"
	"fmt"
	"log"
	"time"

	"github.com/go-redis/redis/v8"
	"github.com/ntons/redlock"
)

func Example() {
	// Connect to redis.
	client := redis.NewClient(&redis.Options{
		Network: "tcp",
		Addr:    "127.0.0.1:6379",
	})
	defer client.Close()

	// Create a new lock client.
	ctx := context.Background()

	// Try to obtain lock.
	lock, err := redlock.Obtain(ctx, client, "my-key", 100*time.Millisecond)
	if err == redlock.ErrNotObtained {
		fmt.Println("Could not obtain lock!")
	} else if err != nil {
		log.Fatalln(err)
	}

	// Don't forget to defer Release.
	defer redlock.Release(ctx, client, lock)
	fmt.Println("I have a lock!")

	// Sleep and check the remaining TTL.
	time.Sleep(50 * time.Millisecond)
	if ttl, err := redlock.TTL(ctx, client, lock); err != nil {
		log.Fatalln(err)
	} else if ttl > 0 {
		fmt.Println("Yay, I still have my lock!")
	}

	// Extend my lock.
	if err := redlock.Refresh(ctx, client, lock, 100*time.Millisecond); err != nil {
		log.Fatalln(err)
	}

	// Sleep a little longer, then check.
	time.Sleep(100 * time.Millisecond)
	if ttl, err := redlock.TTL(ctx, client, lock); err != nil {
		log.Fatalln(err)
	} else if ttl == 0 {
		fmt.Println("Now, my lock has expired!")
	}

	// Output:
	// I have a lock!
	// Yay, I still have my lock!
	// Now, my lock has expired!
}

func ExampleClient_Obtain_retry() {
	client := redis.NewClient(&redis.Options{Network: "tcp", Addr: "127.0.0.1:6379"})
	defer client.Close()

	// Retry every 100ms, for up-to 3x
	backoff := redlock.LimitRetry(redlock.LinearBackoff(100*time.Millisecond), 3)

	ctx := context.Background()

	// Obtain lock with retry
	lock, err := redlock.Obtain(ctx, client, "my-key", time.Second, redlock.WithRetryStrategy(backoff))
	if err == redlock.ErrNotObtained {
		fmt.Println("Could not obtain lock!")
	} else if err != nil {
		log.Fatalln(err)
	}
	defer redlock.Release(ctx, client, lock)

	fmt.Println("I have a lock!")
}

func ExampleClient_Obtain_customDeadline() {
	client := redis.NewClient(&redis.Options{Network: "tcp", Addr: "127.0.0.1:6379"})
	defer client.Close()

	// Retry every 500ms, for up-to a minute
	backoff := redlock.LinearBackoff(500 * time.Millisecond)
	ctx, cancel := context.WithDeadline(context.Background(), time.Now().Add(time.Minute))
	defer cancel()

	// Obtain lock with retry + custom deadline
	lock, err := redlock.Obtain(ctx, client, "my-key", time.Second, redlock.WithRetryStrategy(backoff))
	if err == redlock.ErrNotObtained {
		fmt.Println("Could not obtain lock!")
	} else if err != nil {
		log.Fatalln(err)
	}
	defer redlock.Release(ctx, client, lock)

	fmt.Println("I have a lock!")
}
