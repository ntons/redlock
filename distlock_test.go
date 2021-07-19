package distlock_test

import (
	"context"
	"math/rand"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/go-redis/redis/v8"
	"github.com/ntons/distlock"
	. "github.com/onsi/ginkgo"
	. "github.com/onsi/gomega"
)

const lockKey = "__ntons_distlock_go_unit_test__"

var _ = Describe("Client", func() {
	var ctx = context.Background()

	AfterEach(func() {
		Expect(redisClient.Del(ctx, lockKey).Err()).To(Succeed())
	})

	It("should obtain once with TTL", func() {
		lock1, err := distlock.Obtain(ctx, redisClient, lockKey, time.Hour)
		Expect(err).NotTo(HaveOccurred())
		Expect(lock1.GetToken()).To(HaveLen(22))
		Expect(distlock.TTL(ctx, redisClient, lock1)).To(BeNumerically("~", time.Hour, time.Second))
		defer distlock.Release(ctx, redisClient, lock1)

		_, err = distlock.Obtain(ctx, redisClient, lockKey, time.Hour)
		Expect(err).To(Equal(distlock.ErrNotObtained))
		Expect(distlock.Release(ctx, redisClient, lock1)).To(Succeed())

		lock2, err := distlock.Obtain(ctx, redisClient, lockKey, time.Minute)
		Expect(err).NotTo(HaveOccurred())
		Expect(distlock.TTL(ctx, redisClient, lock2)).To(BeNumerically("~", time.Minute, time.Second))
		Expect(distlock.Release(ctx, redisClient, lock2)).To(Succeed())
	})

	It("should obtain through short-cut", func() {
		lock, err := distlock.Obtain(ctx, redisClient, lockKey, time.Hour)
		Expect(err).NotTo(HaveOccurred())
		Expect(distlock.Release(ctx, redisClient, lock)).To(Succeed())
	})

	//It("should support custom metadata", func() {
	//	lock, err := distlock.Obtain(ctx, redisClient, lockKey, time.Hour, distlock.WithMetadata("my-data"))
	//	Expect(err).NotTo(HaveOccurred())
	//	Expect(lock.GetMetadata()).To(Equal("my-data"))
	//	Expect(distlock.Release(ctx, redisClient, lock)).To(Succeed())
	//})

	It("should refresh", func() {
		lock, err := distlock.Obtain(ctx, redisClient, lockKey, time.Minute)
		Expect(err).NotTo(HaveOccurred())
		Expect(distlock.TTL(ctx, redisClient, lock)).To(BeNumerically("~", time.Minute, time.Second))
		Expect(distlock.Refresh(ctx, redisClient, lock, time.Hour)).To(Succeed())
		Expect(distlock.TTL(ctx, redisClient, lock)).To(BeNumerically("~", time.Hour, time.Second))
		Expect(distlock.Release(ctx, redisClient, lock)).To(Succeed())
	})

	It("should ensure", func() {
		lock, err := distlock.Obtain(ctx, redisClient, lockKey, time.Minute)
		Expect(err).NotTo(HaveOccurred())
		Expect(distlock.TTL(ctx, redisClient, lock)).To(BeNumerically("~", time.Minute, time.Second))
		Expect(distlock.Ensure(ctx, redisClient, lock, time.Hour)).To(Succeed())
		Expect(distlock.TTL(ctx, redisClient, lock)).To(BeNumerically("~", time.Hour, time.Second))
		Expect(distlock.Ensure(ctx, redisClient, lock, time.Minute)).To(Succeed())
		Expect(distlock.TTL(ctx, redisClient, lock)).To(BeNumerically("~", time.Hour, time.Second))
		Expect(distlock.Release(ctx, redisClient, lock)).To(Succeed())
	})

	It("should fail to release if expired", func() {
		lock, err := distlock.Obtain(ctx, redisClient, lockKey, 5*time.Millisecond)
		Expect(err).NotTo(HaveOccurred())
		time.Sleep(6 * time.Millisecond)
		Expect(distlock.Release(ctx, redisClient, lock)).To(MatchError(distlock.ErrLockNotHeld))
	})

	It("should fail to release if ontained by someone else", func() {
		lock, err := distlock.Obtain(ctx, redisClient, lockKey, time.Minute)
		Expect(err).NotTo(HaveOccurred())

		Expect(redisClient.Set(ctx, lockKey, "ABCD", 0).Err()).NotTo(HaveOccurred())
		Expect(distlock.Release(ctx, redisClient, lock)).To(MatchError(distlock.ErrLockNotHeld))
	})

	It("should fail to refresh if expired", func() {
		lock, err := distlock.Obtain(ctx, redisClient, lockKey, 5*time.Millisecond)
		Expect(err).NotTo(HaveOccurred())
		time.Sleep(6 * time.Millisecond)
		Expect(distlock.Refresh(ctx, redisClient, lock, time.Hour)).To(MatchError(distlock.ErrNotObtained))
	})

	It("should retry if enabled", func() {
		// retry, succeed
		Expect(redisClient.Set(ctx, lockKey, "ABCD", 0).Err()).NotTo(HaveOccurred())
		Expect(redisClient.PExpire(ctx, lockKey, 20*time.Millisecond).Err()).NotTo(HaveOccurred())

		lock, err := distlock.Obtain(ctx, redisClient, lockKey, time.Hour, distlock.WithRetryStrategy(distlock.LimitRetry(distlock.LinearBackoff(100*time.Millisecond), 3)))
		Expect(err).NotTo(HaveOccurred())
		Expect(distlock.Release(ctx, redisClient, lock)).To(Succeed())

		// no retry, fail
		Expect(redisClient.Set(ctx, lockKey, "ABCD", 0).Err()).NotTo(HaveOccurred())
		Expect(redisClient.PExpire(ctx, lockKey, 50*time.Millisecond).Err()).NotTo(HaveOccurred())

		_, err = distlock.Obtain(ctx, redisClient, lockKey, time.Hour)
		Expect(err).To(MatchError(distlock.ErrNotObtained))

		// retry 2x, give up & fail
		Expect(redisClient.Set(ctx, lockKey, "ABCD", 0).Err()).NotTo(HaveOccurred())
		Expect(redisClient.PExpire(ctx, lockKey, 50*time.Millisecond).Err()).NotTo(HaveOccurred())

		_, err = distlock.Obtain(ctx, redisClient, lockKey, time.Hour, distlock.WithRetryStrategy(distlock.LimitRetry(distlock.LinearBackoff(time.Millisecond), 2)))
		Expect(err).To(MatchError(distlock.ErrNotObtained))
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

				_, err := distlock.Obtain(ctx, redisClient, lockKey, time.Minute)
				if err == distlock.ErrNotObtained {
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
		subject := distlock.NoRetry()
		Expect(subject.NextBackoff()).To(Equal(time.Duration(0)))
	})

	It("should support linear backoff", func() {
		subject := distlock.LinearBackoff(time.Second)
		Expect(subject.NextBackoff()).To(Equal(time.Second))
		Expect(subject.NextBackoff()).To(Equal(time.Second))
	})

	It("should support limits", func() {
		subject := distlock.LimitRetry(distlock.LinearBackoff(time.Second), 2)
		Expect(subject.NextBackoff()).To(Equal(time.Second))
		Expect(subject.NextBackoff()).To(Equal(time.Second))
		Expect(subject.NextBackoff()).To(Equal(time.Duration(0)))
	})

	It("should support exponential backoff", func() {
		subject := distlock.ExponentialBackoff(10*time.Millisecond, 300*time.Millisecond)
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
	RunSpecs(t, "distlock")
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
