package shardcoordinator

import (
	"context"
	"math/rand"
	"time"
)

// TrackingLocker wraps a Locker to track method calls with timestamps.
type TrackingLocker struct {
	underlying   Locker
	onTryAcquire func(time.Time)
	onRenew      func(time.Time)
	onRelease    func(time.Time)
}

func (t *TrackingLocker) TryAcquire(ctx context.Context, shardID, ownerID string, ttl time.Time) (bool, error) {
	if t.onTryAcquire != nil {
		t.onTryAcquire(time.Now())
	}
	return t.underlying.TryAcquire(ctx, shardID, ownerID, ttl)
}

func (t *TrackingLocker) Renew(ctx context.Context, shardID, ownerID string, ttl time.Time) (bool, error) {
	if t.onRenew != nil {
		t.onRenew(time.Now())
	}
	return t.underlying.Renew(ctx, shardID, ownerID, ttl)
}

func (t *TrackingLocker) Release(ctx context.Context, shardID, ownerID string) error {
	if t.onRelease != nil {
		t.onRelease(time.Now())
	}
	return t.underlying.Release(ctx, shardID, ownerID)
}

// DelayedLocker wraps a Locker with artificial delays to simulate slow backends.
type DelayedLocker struct {
	underlying   Locker
	acquireDelay time.Duration
	renewDelay   time.Duration
	releaseDelay time.Duration
}

func (d *DelayedLocker) TryAcquire(ctx context.Context, shardID, ownerID string, ttl time.Time) (bool, error) {
	if d.acquireDelay > 0 {
		time.Sleep(d.acquireDelay)
	}
	return d.underlying.TryAcquire(ctx, shardID, ownerID, ttl)
}

func (d *DelayedLocker) Renew(ctx context.Context, shardID, ownerID string, ttl time.Time) (bool, error) {
	if d.renewDelay > 0 {
		time.Sleep(d.renewDelay)
	}
	return d.underlying.Renew(ctx, shardID, ownerID, ttl)
}

func (d *DelayedLocker) Release(ctx context.Context, shardID, ownerID string) error {
	if d.releaseDelay > 0 {
		time.Sleep(d.releaseDelay)
	}
	return d.underlying.Release(ctx, shardID, ownerID)
}

// FlakyLocker randomly fails operations for chaos testing.
// It wraps an underlying locker and randomly returns errors based on failureRate.
type FlakyLocker struct {
	underlying  Locker
	failureRate float64 // 0.0 to 1.0 probability of failure
}

func (f *FlakyLocker) TryAcquire(ctx context.Context, shardID, ownerID string, ttl time.Time) (bool, error) {
	if rand.Float64() < f.failureRate {
		return false, nil
	}
	return f.underlying.TryAcquire(ctx, shardID, ownerID, ttl)
}

func (f *FlakyLocker) Renew(ctx context.Context, shardID, ownerID string, ttl time.Time) (bool, error) {
	if rand.Float64() < f.failureRate {
		return false, nil
	}
	return f.underlying.Renew(ctx, shardID, ownerID, ttl)
}

func (f *FlakyLocker) Release(ctx context.Context, shardID, ownerID string) error {
	if rand.Float64() < f.failureRate {
		return nil
	}
	return f.underlying.Release(ctx, shardID, ownerID)
}
