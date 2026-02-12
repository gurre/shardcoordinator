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
	})

	t.Run("TryAcquire on held lock", func(t *testing.T) {
		locker := newLocker(t)
		ctx := context.Background()
		ttl := time.Now().Add(30 * time.Second)

		// First acquisition should succeed
		acquired, err := locker.TryAcquire(ctx, "shard-2", "owner-1", ttl)
		if err != nil {
			t.Fatalf("First TryAcquire() error = %v", err)
		}
		if !acquired {
			t.Fatal("First TryAcquire() should succeed")
		}

		// Second acquisition by different owner should fail
		acquired, err = locker.TryAcquire(ctx, "shard-2", "owner-2", ttl)
		if err != nil {
			t.Fatalf("Second TryAcquire() error = %v", err)
		}
		if acquired {
			t.Error("TryAcquire() should fail when lock is held by another owner")
		}
	})

	t.Run("TryAcquire on expired lock", func(t *testing.T) {
		locker := newLocker(t)
		ctx := context.Background()
		expiredTTL := time.Now().Add(-1 * time.Second)
		validTTL := time.Now().Add(30 * time.Second)

		// Acquire with expired TTL
		acquired, err := locker.TryAcquire(ctx, "shard-3", "owner-1", expiredTTL)
		if err != nil {
			t.Fatalf("TryAcquire() error = %v", err)
		}
		if !acquired {
			t.Fatal("TryAcquire() should succeed")
		}

		// Different owner should be able to take over expired lock
		acquired, err = locker.TryAcquire(ctx, "shard-3", "owner-2", validTTL)
		if err != nil {
			t.Fatalf("TryAcquire() on expired lock error = %v", err)
		}
		if !acquired {
			t.Error("TryAcquire() should succeed on expired lock")
		}
	})

	t.Run("Renew by owner", func(t *testing.T) {
		locker := newLocker(t)
		ctx := context.Background()
		ttl1 := time.Now().Add(30 * time.Second)

		// Acquire lock
		acquired, err := locker.TryAcquire(ctx, "shard-4", "owner-1", ttl1)
		if err != nil {
			t.Fatalf("TryAcquire() error = %v", err)
		}
		if !acquired {
			t.Fatal("TryAcquire() should succeed")
		}

		// Renew by owner
		ttl2 := time.Now().Add(60 * time.Second)
		renewed, err := locker.Renew(ctx, "shard-4", "owner-1", ttl2)
		if err != nil {
			t.Fatalf("Renew() error = %v", err)
		}
		if !renewed {
			t.Error("Renew() should succeed for owner")
		}
	})

	t.Run("Renew by non-owner", func(t *testing.T) {
		locker := newLocker(t)
		ctx := context.Background()
		ttl := time.Now().Add(30 * time.Second)

		// Acquire lock
		acquired, err := locker.TryAcquire(ctx, "shard-5", "owner-1", ttl)
		if err != nil {
			t.Fatalf("TryAcquire() error = %v", err)
		}
		if !acquired {
			t.Fatal("TryAcquire() should succeed")
		}

		// Renew by non-owner should fail
		renewed, err := locker.Renew(ctx, "shard-5", "owner-2", ttl)
		if err != nil {
			t.Fatalf("Renew() error = %v", err)
		}
		if renewed {
			t.Error("Renew() should fail for non-owner")
		}
	})

	t.Run("Renew after expiration", func(t *testing.T) {
		locker := newLocker(t)
		ctx := context.Background()
		expiredTTL := time.Now().Add(-1 * time.Second)

		// Acquire lock with expired TTL
		acquired, err := locker.TryAcquire(ctx, "shard-6", "owner-1", expiredTTL)
		if err != nil {
			t.Fatalf("TryAcquire() error = %v", err)
		}
		if !acquired {
			t.Fatal("TryAcquire() should succeed")
		}

		// Renew should fail because lock is expired
		validTTL := time.Now().Add(30 * time.Second)
		renewed, err := locker.Renew(ctx, "shard-6", "owner-1", validTTL)
		if err != nil {
			t.Fatalf("Renew() error = %v", err)
		}
		if renewed {
			t.Error("Renew() should fail for expired lock")
		}
	})

	t.Run("Release by owner", func(t *testing.T) {
		locker := newLocker(t)
		ctx := context.Background()
		ttl := time.Now().Add(30 * time.Second)

		// Acquire lock
		acquired, err := locker.TryAcquire(ctx, "shard-7", "owner-1", ttl)
		if err != nil {
			t.Fatalf("TryAcquire() error = %v", err)
		}
		if !acquired {
			t.Fatal("TryAcquire() should succeed")
		}

		// Release by owner
		err = locker.Release(ctx, "shard-7", "owner-1")
		if err != nil {
			t.Fatalf("Release() error = %v", err)
		}

		// After release, another owner should be able to acquire
		acquired, err = locker.TryAcquire(ctx, "shard-7", "owner-2", ttl)
		if err != nil {
			t.Fatalf("TryAcquire() after release error = %v", err)
		}
		if !acquired {
			t.Error("TryAcquire() should succeed after release")
		}
	})

	t.Run("Release by non-owner", func(t *testing.T) {
		locker := newLocker(t)
		ctx := context.Background()
		ttl := time.Now().Add(30 * time.Second)

		// Acquire lock
		acquired, err := locker.TryAcquire(ctx, "shard-8", "owner-1", ttl)
		if err != nil {
			t.Fatalf("TryAcquire() error = %v", err)
		}
		if !acquired {
			t.Fatal("TryAcquire() should succeed")
		}

		// Release by non-owner should not error
		err = locker.Release(ctx, "shard-8", "owner-2")
		if err != nil {
			t.Fatalf("Release() by non-owner should not error, got: %v", err)
		}

		// Lock should still be held by original owner
		// (cannot be acquired by a third owner)
		acquired, err = locker.TryAcquire(ctx, "shard-8", "owner-3", ttl)
		if err != nil {
			t.Fatalf("TryAcquire() error = %v", err)
		}
		if acquired {
			t.Error("Lock should still be held after non-owner release attempt")
		}
	})

	t.Run("Concurrent TryAcquire - only one winner", func(t *testing.T) {
		locker := newLocker(t)
		ctx := context.Background()
		ttl := time.Now().Add(30 * time.Second)

		const numGoroutines = 10
		var successCount int32
		var wg sync.WaitGroup
		wg.Add(numGoroutines)

		// Start multiple goroutines trying to acquire the same lock
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

		// Only one goroutine should have successfully acquired the lock
		if successCount != 1 {
			t.Errorf("Expected exactly 1 successful acquisition, got %d", successCount)
		}
	})
}
