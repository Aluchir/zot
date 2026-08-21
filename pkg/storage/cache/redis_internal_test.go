package cache

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/alicebob/miniredis/v2"
	"github.com/go-redsync/redsync/v4"
	gors "github.com/go-redsync/redsync/v4/redis/goredis/v9"
	"github.com/redis/go-redis/v9"
	. "github.com/smartystreets/goconvey/convey"

	zerr "zotregistry.dev/zot/v2/errors"
	"zotregistry.dev/zot/v2/pkg/log"
)

func newTestRedisDriver(t *testing.T, miniRedis *miniredis.Miniredis) *RedisDriver {
	t.Helper()

	client := redis.NewClient(&redis.Options{Addr: miniRedis.Addr()})
	t.Cleanup(func() { _ = client.Close() })

	return &RedisDriver{
		db:        client,
		log:       log.NewTestLogger(),
		keyPrefix: "zot",
		rs:        redsync.New(gors.NewPool(client)),
	}
}

func TestLockRepoMutualExclusion(t *testing.T) {
	Convey("A second holder cannot take the lock until the first releases", t, func() {
		miniRedis := miniredis.RunT(t)
		first := newTestRedisDriver(t, miniRedis)
		second := newTestRedisDriver(t, miniRedis)

		ctx := context.Background()

		unlock, err := first.LockRepo(ctx, "repo")
		So(err, ShouldBeNil)

		waitCtx, cancel := context.WithTimeout(ctx, 300*time.Millisecond)
		defer cancel()

		_, err = second.LockRepo(waitCtx, "repo")
		So(errors.Is(err, zerr.ErrRepoLockUnavailable), ShouldBeTrue)

		unlock()

		unlockSecond, err := second.LockRepo(ctx, "repo")
		So(err, ShouldBeNil)
		unlockSecond()
	})

	Convey("An already-canceled context fails acquisition immediately", t, func() {
		miniRedis := miniredis.RunT(t)
		driver := newTestRedisDriver(t, miniRedis)

		ctx, cancel := context.WithCancel(context.Background())
		cancel()

		_, err := driver.LockRepo(ctx, "repo")
		So(errors.Is(err, zerr.ErrRepoLockUnavailable), ShouldBeTrue)
	})
}

func TestLockRepoRenewsWhileHeld(t *testing.T) {
	Convey("The lock stays exclusive past its initial expiry because it is renewed", t, func() {
		miniRedis := miniredis.RunT(t)
		first := newTestRedisDriver(t, miniRedis)
		second := newTestRedisDriver(t, miniRedis)

		// shrink the lock timings so the test can outlive the initial expiry
		defer func() {
			repoLockExpiry = 30 * time.Second
			repoLockRetryDelay = 250 * time.Millisecond
		}()

		repoLockExpiry = 600 * time.Millisecond
		repoLockRetryDelay = 50 * time.Millisecond

		ctx := context.Background()

		unlock, err := first.LockRepo(ctx, "repo")
		So(err, ShouldBeNil)

		// well past the initial 600ms expiry; only the renewal can still be holding it
		time.Sleep(time.Second)

		waitCtx, cancel := context.WithTimeout(ctx, 200*time.Millisecond)
		defer cancel()

		_, err = second.LockRepo(waitCtx, "repo")
		So(errors.Is(err, zerr.ErrRepoLockUnavailable), ShouldBeTrue)

		unlock()

		unlockSecond, err := second.LockRepo(ctx, "repo")
		So(err, ShouldBeNil)
		unlockSecond()
	})
}

func TestLockRepoReleaseAfterRedisDown(t *testing.T) {
	Convey("Renewal and release failures are logged, not panicked on", t, func() {
		miniRedis := miniredis.RunT(t)
		driver := newTestRedisDriver(t, miniRedis)

		defer func() {
			repoLockExpiry = 30 * time.Second
			repoLockRetryDelay = 250 * time.Millisecond
		}()

		repoLockExpiry = 300 * time.Millisecond
		repoLockRetryDelay = 50 * time.Millisecond

		unlock, err := driver.LockRepo(context.Background(), "repo")
		So(err, ShouldBeNil)

		miniRedis.Close()

		// let a renewal tick run against the dead server, then release into it too
		time.Sleep(300 * time.Millisecond)

		So(func() { unlock() }, ShouldNotPanic)
	})
}
