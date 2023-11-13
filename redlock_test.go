package redlock_test

import (
	"context"
	"math/rand"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/go-redis/redis/v8"
	"github.com/ntons/redlock"
	. "github.com/onsi/ginkgo"
	. "github.com/onsi/gomega"
)

const lockKey = "__ntons_redlock_go_unit_test__"

var _ = Describe("Client", func() {
	var ctx = context.Background()

	AfterEach(func() {
		Expect(redisClient.Del(ctx, lockKey).Err()).To(Succeed())
	})

	It("should obtain once with TTL", func() {
		lock1, err := redlock.Obtain(ctx, redisClient, lockKey, time.Hour)
		Expect(err).NotTo(HaveOccurred())
		Expect(lock1.GetToken()).To(HaveLen(22))
		Expect(redlock.TTL(ctx, redisClient, lock1)).To(BeNumerically("~", time.Hour, time.Second))
		defer redlock.Release(ctx, redisClient, lock1)

		_, err = redlock.Obtain(ctx, redisClient, lockKey, time.Hour)
		Expect(err).To(Equal(redlock.ErrNotObtained))
		Expect(redlock.Release(ctx, redisClient, lock1)).To(Succeed())

		lock2, err := redlock.Obtain(ctx, redisClient, lockKey, time.Minute)
		Expect(err).NotTo(HaveOccurred())
		Expect(redlock.TTL(ctx, redisClient, lock2)).To(BeNumerically("~", time.Minute, time.Second))
		Expect(redlock.Release(ctx, redisClient, lock2)).To(Succeed())
	})

	It("should obtain through short-cut", func() {
		lock, err := redlock.Obtain(ctx, redisClient, lockKey, time.Hour)
		Expect(err).NotTo(HaveOccurred())
		Expect(redlock.Release(ctx, redisClient, lock)).To(Succeed())
	})

	//It("should support custom metadata", func() {
	//	lock, err := redlock.Obtain(ctx, redisClient, lockKey, time.Hour, redlock.WithMetadata("my-data"))
	//	Expect(err).NotTo(HaveOccurred())
	//	Expect(lock.GetMetadata()).To(Equal("my-data"))
	//	Expect(redlock.Release(ctx, redisClient, lock)).To(Succeed())
	//})

	It("should refresh", func() {
		lock, err := redlock.Obtain(ctx, redisClient, lockKey, time.Minute)
		Expect(err).NotTo(HaveOccurred())
		Expect(redlock.TTL(ctx, redisClient, lock)).To(BeNumerically("~", time.Minute, time.Second))
		Expect(redlock.Refresh(ctx, redisClient, lock, time.Hour)).To(Succeed())
		Expect(redlock.TTL(ctx, redisClient, lock)).To(BeNumerically("~", time.Hour, time.Second))
		Expect(redlock.Release(ctx, redisClient, lock)).To(Succeed())
	})

	It("should ensure", func() {
		lock, err := redlock.Obtain(ctx, redisClient, lockKey, time.Minute)
		Expect(err).NotTo(HaveOccurred())
		Expect(redlock.TTL(ctx, redisClient, lock)).To(BeNumerically("~", time.Minute, time.Second))
		Expect(redlock.Ensure(ctx, redisClient, lock, time.Hour)).To(Succeed())
		Expect(redlock.TTL(ctx, redisClient, lock)).To(BeNumerically("~", time.Hour, time.Second))
		Expect(redlock.Ensure(ctx, redisClient, lock, time.Minute)).To(Succeed())
		Expect(redlock.TTL(ctx, redisClient, lock)).To(BeNumerically("~", time.Hour, time.Second))
		Expect(redlock.Release(ctx, redisClient, lock)).To(Succeed())
	})

	It("should fail to release if expired", func() {
		lock, err := redlock.Obtain(ctx, redisClient, lockKey, 5*time.Millisecond)
		Expect(err).NotTo(HaveOccurred())
		time.Sleep(6 * time.Millisecond)
		Expect(redlock.Release(ctx, redisClient, lock)).To(MatchError(redlock.ErrLockNotHeld))
	})

	It("should fail to release if ontained by someone else", func() {
		lock, err := redlock.Obtain(ctx, redisClient, lockKey, time.Minute)
		Expect(err).NotTo(HaveOccurred())

		Expect(redisClient.Set(ctx, lockKey, "ABCD", 0).Err()).NotTo(HaveOccurred())
		Expect(redlock.Release(ctx, redisClient, lock)).To(MatchError(redlock.ErrLockNotHeld))
	})

	It("should fail to refresh if expired", func() {
		lock, err := redlock.Obtain(ctx, redisClient, lockKey, 5*time.Millisecond)
		Expect(err).NotTo(HaveOccurred())
		time.Sleep(6 * time.Millisecond)
		Expect(redlock.Refresh(ctx, redisClient, lock, time.Hour)).To(MatchError(redlock.ErrNotObtained))
	})

	It("should retry if enabled", func() {
		// retry, succeed
		Expect(redisClient.Set(ctx, lockKey, "ABCD", 0).Err()).NotTo(HaveOccurred())
		Expect(redisClient.PExpire(ctx, lockKey, 20*time.Millisecond).Err()).NotTo(HaveOccurred())

		lock, err := redlock.Obtain(ctx, redisClient, lockKey, time.Hour, redlock.WithRetryStrategy(redlock.LimitRetry(redlock.LinearBackoff(100*time.Millisecond), 3)))
		Expect(err).NotTo(HaveOccurred())
		Expect(redlock.Release(ctx, redisClient, lock)).To(Succeed())

		// no retry, fail
		Expect(redisClient.Set(ctx, lockKey, "ABCD", 0).Err()).NotTo(HaveOccurred())
		Expect(redisClient.PExpire(ctx, lockKey, 50*time.Millisecond).Err()).NotTo(HaveOccurred())

		_, err = redlock.Obtain(ctx, redisClient, lockKey, time.Hour)
		Expect(err).To(MatchError(redlock.ErrNotObtained))

		// retry 2x, give up & fail
		Expect(redisClient.Set(ctx, lockKey, "ABCD", 0).Err()).NotTo(HaveOccurred())
		Expect(redisClient.PExpire(ctx, lockKey, 50*time.Millisecond).Err()).NotTo(HaveOccurred())

		_, err = redlock.Obtain(ctx, redisClient, lockKey, time.Hour, redlock.WithRetryStrategy(redlock.LimitRetry(redlock.LinearBackoff(time.Millisecond), 2)))
		Expect(err).To(MatchError(redlock.ErrNotObtained))
	})

	It("should prevent multiple locks (fuzzing)", func() {
		numLocks := int32(0)
		wg := new(sync.WaitGroup)
		for i := 0; i < 1000; i++ {
			wg.Add(1)

			go func() {
				defer GinkgoRecover()
				defer wg.Done()

				wait := rand.Int63n(int64(50 * time.Millisecond))
				time.Sleep(time.Duration(wait))

				_, err := redlock.Obtain(ctx, redisClient, lockKey, time.Minute)
				if err == redlock.ErrNotObtained {
					return
				}
				Expect(err).NotTo(HaveOccurred())
				atomic.AddInt32(&numLocks, 1)
			}()
		}
		wg.Wait()
		Expect(numLocks).To(Equal(int32(1)))
	})

})

var _ = Describe("RetryStrategy", func() {
	It("should support no-retry", func() {
		subject := redlock.NoRetry()
		Expect(subject.NextBackoff()).To(Equal(time.Duration(0)))
	})

	It("should support linear backoff", func() {
		subject := redlock.LinearBackoff(time.Second)
		Expect(subject.NextBackoff()).To(Equal(time.Second))
		Expect(subject.NextBackoff()).To(Equal(time.Second))
	})

	It("should support limits", func() {
		subject := redlock.LimitRetry(redlock.LinearBackoff(time.Second), 2)
		Expect(subject.NextBackoff()).To(Equal(time.Second))
		Expect(subject.NextBackoff()).To(Equal(time.Second))
		Expect(subject.NextBackoff()).To(Equal(time.Duration(0)))
	})

	It("should support exponential backoff", func() {
		subject := redlock.ExponentialBackoff(10*time.Millisecond, 300*time.Millisecond)
		Expect(subject.NextBackoff()).To(Equal(10 * time.Millisecond))
		Expect(subject.NextBackoff()).To(Equal(10 * time.Millisecond))
		Expect(subject.NextBackoff()).To(Equal(16 * time.Millisecond))
		Expect(subject.NextBackoff()).To(Equal(32 * time.Millisecond))
		Expect(subject.NextBackoff()).To(Equal(64 * time.Millisecond))
		Expect(subject.NextBackoff()).To(Equal(128 * time.Millisecond))
		Expect(subject.NextBackoff()).To(Equal(256 * time.Millisecond))
		Expect(subject.NextBackoff()).To(Equal(300 * time.Millisecond))
		Expect(subject.NextBackoff()).To(Equal(300 * time.Millisecond))
		Expect(subject.NextBackoff()).To(Equal(300 * time.Millisecond))
	})
})

// --------------------------------------------------------------------

func TestSuite(t *testing.T) {
	RegisterFailHandler(Fail)
	RunSpecs(t, "redlock")
}

var redisClient *redis.Client

var _ = BeforeSuite(func() {
	redisClient = redis.NewClient(&redis.Options{
		Network: "tcp",
		Addr:    "127.0.0.1:6379", DB: 9,
	})
	Expect(redisClient.Ping(context.Background()).Err()).To(Succeed())
})

var _ = AfterSuite(func() {
	Expect(redisClient.Close()).To(Succeed())
})
