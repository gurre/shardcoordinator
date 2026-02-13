package shardcoordinator

import (
	"context"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

// LockerConformanceTest validates that a Locker implementation conforms to the expected behavior.
// This test suite ensures that all backend implementations provide identical guarantees.
func LockerConformanceTest(t *testing.T, newLocker func(t *testing.T) Locker) {
	t.Run("TryAcquire on non-existent lock", func(t *testing.T) {
		testTryAcquireNonExistent(t, newLocker)
	})

	t.Run("TryAcquire on held lock", func(t *testing.T) {
		testTryAcquireHeldLock(t, newLocker)
	})

	t.Run("TryAcquire on expired lock", func(t *testing.T) {
		testTryAcquireExpiredLock(t, newLocker)
	})

	t.Run("Renew by owner", func(t *testing.T) {
		testRenewByOwner(t, newLocker)
	})

	t.Run("Renew by non-owner", func(t *testing.T) {
		testRenewByNonOwner(t, newLocker)
	})

	t.Run("Renew after expiration", func(t *testing.T) {
		testRenewAfterExpiration(t, newLocker)
	})

	t.Run("Release by owner", func(t *testing.T) {
		testReleaseByOwner(t, newLocker)
	})

	t.Run("Release by non-owner", func(t *testing.T) {
		testReleaseByNonOwner(t, newLocker)
	})

	t.Run("Concurrent TryAcquire - only one winner", func(t *testing.T) {
		testConcurrentTryAcquire(t, newLocker)
	})
}

func testTryAcquireNonExistent(t *testing.T, newLocker func(t *testing.T) Locker) {
	locker := newLocker(t)
	ctx := context.Background()
	ttl := time.Now().Add(30 * time.Second)

	acquired, err := locker.TryAcquire(ctx, "shard-1", "owner-1", ttl)
	if err != nil {
		t.Fatalf("TryAcquire() error = %v", err)
	}
	if !acquired {
		t.Error("TryAcquire() should succeed on non-existent lock")
	}
}

func testTryAcquireHeldLock(t *testing.T, newLocker func(t *testing.T) Locker) {
	locker := newLocker(t)
	ctx := context.Background()
	ttl := time.Now().Add(30 * time.Second)

	acquired, err := locker.TryAcquire(ctx, "shard-2", "owner-1", ttl)
	if err != nil {
		t.Fatalf("First TryAcquire() error = %v", err)
	}
	if !acquired {
		t.Fatal("First TryAcquire() should succeed")
	}

	acquired, err = locker.TryAcquire(ctx, "shard-2", "owner-2", ttl)
	if err != nil {
		t.Fatalf("Second TryAcquire() error = %v", err)
	}
	if acquired {
		t.Error("TryAcquire() should fail when lock is held by another owner")
	}
}

func testTryAcquireExpiredLock(t *testing.T, newLocker func(t *testing.T) Locker) {
	locker := newLocker(t)
	ctx := context.Background()
	expiredTTL := time.Now().Add(-1 * time.Second)
	validTTL := time.Now().Add(30 * time.Second)

	acquired, err := locker.TryAcquire(ctx, "shard-3", "owner-1", expiredTTL)
	if err != nil {
		t.Fatalf("TryAcquire() error = %v", err)
	}
	if !acquired {
		t.Fatal("TryAcquire() should succeed")
	}

	acquired, err = locker.TryAcquire(ctx, "shard-3", "owner-2", validTTL)
	if err != nil {
		t.Fatalf("TryAcquire() on expired lock error = %v", err)
	}
	if !acquired {
		t.Error("TryAcquire() should succeed on expired lock")
	}
}

func testRenewByOwner(t *testing.T, newLocker func(t *testing.T) Locker) {
	locker := newLocker(t)
	ctx := context.Background()
	ttl1 := time.Now().Add(30 * time.Second)

	acquired, err := locker.TryAcquire(ctx, "shard-4", "owner-1", ttl1)
	if err != nil {
		t.Fatalf("TryAcquire() error = %v", err)
	}
	if !acquired {
		t.Fatal("TryAcquire() should succeed")
	}

	ttl2 := time.Now().Add(60 * time.Second)
	renewed, err := locker.Renew(ctx, "shard-4", "owner-1", ttl2)
	if err != nil {
		t.Fatalf("Renew() error = %v", err)
	}
	if !renewed {
		t.Error("Renew() should succeed for owner")
	}
}

func testRenewByNonOwner(t *testing.T, newLocker func(t *testing.T) Locker) {
	locker := newLocker(t)
	ctx := context.Background()
	ttl := time.Now().Add(30 * time.Second)

	acquired, err := locker.TryAcquire(ctx, "shard-5", "owner-1", ttl)
	if err != nil {
		t.Fatalf("TryAcquire() error = %v", err)
	}
	if !acquired {
		t.Fatal("TryAcquire() should succeed")
	}

	renewed, err := locker.Renew(ctx, "shard-5", "owner-2", ttl)
	if err != nil {
		t.Fatalf("Renew() error = %v", err)
	}
	if renewed {
		t.Error("Renew() should fail for non-owner")
	}
}

func testRenewAfterExpiration(t *testing.T, newLocker func(t *testing.T) Locker) {
	locker := newLocker(t)
	ctx := context.Background()
	expiredTTL := time.Now().Add(-1 * time.Second)

	acquired, err := locker.TryAcquire(ctx, "shard-6", "owner-1", expiredTTL)
	if err != nil {
		t.Fatalf("TryAcquire() error = %v", err)
	}
	if !acquired {
		t.Fatal("TryAcquire() should succeed")
	}

	validTTL := time.Now().Add(30 * time.Second)
	renewed, err := locker.Renew(ctx, "shard-6", "owner-1", validTTL)
	if err != nil {
		t.Fatalf("Renew() error = %v", err)
	}
	if renewed {
		t.Error("Renew() should fail for expired lock")
	}
}

func testReleaseByOwner(t *testing.T, newLocker func(t *testing.T) Locker) {
	locker := newLocker(t)
	ctx := context.Background()
	ttl := time.Now().Add(30 * time.Second)

	acquired, err := locker.TryAcquire(ctx, "shard-7", "owner-1", ttl)
	if err != nil {
		t.Fatalf("TryAcquire() error = %v", err)
	}
	if !acquired {
		t.Fatal("TryAcquire() should succeed")
	}

	err = locker.Release(ctx, "shard-7", "owner-1")
	if err != nil {
		t.Fatalf("Release() error = %v", err)
	}

	acquired, err = locker.TryAcquire(ctx, "shard-7", "owner-2", ttl)
	if err != nil {
		t.Fatalf("TryAcquire() after release error = %v", err)
	}
	if !acquired {
		t.Error("TryAcquire() should succeed after release")
	}
}

func testReleaseByNonOwner(t *testing.T, newLocker func(t *testing.T) Locker) {
	locker := newLocker(t)
	ctx := context.Background()
	ttl := time.Now().Add(30 * time.Second)

	acquired, err := locker.TryAcquire(ctx, "shard-8", "owner-1", ttl)
	if err != nil {
		t.Fatalf("TryAcquire() error = %v", err)
	}
	if !acquired {
		t.Fatal("TryAcquire() should succeed")
	}

	err = locker.Release(ctx, "shard-8", "owner-2")
	if err != nil {
		t.Fatalf("Release() by non-owner should not error, got: %v", err)
	}

	acquired, err = locker.TryAcquire(ctx, "shard-8", "owner-3", ttl)
	if err != nil {
		t.Fatalf("TryAcquire() error = %v", err)
	}
	if acquired {
		t.Error("Lock should still be held after non-owner release attempt")
	}
}

func testConcurrentTryAcquire(t *testing.T, newLocker func(t *testing.T) Locker) {
	locker := newLocker(t)
	ctx := context.Background()
	ttl := time.Now().Add(30 * time.Second)

	const numGoroutines = 10
	var successCount int32
	var wg sync.WaitGroup
	wg.Add(numGoroutines)

	for i := 0; i < numGoroutines; i++ {
		go func(ownerID int) {
			defer wg.Done()

			acquired, err := locker.TryAcquire(ctx, "shard-9", "owner-"+string(rune('0'+ownerID)), ttl)
			if err != nil {
				t.Logf("TryAcquire() error = %v", err)
				return
			}
			if acquired {
				atomic.AddInt32(&successCount, 1)
			}
		}(i)
	}

	wg.Wait()

	if successCount != 1 {
		t.Errorf("Expected exactly 1 successful acquisition, got %d", successCount)
	}
}
