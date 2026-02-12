package shardcoordinator

import (
	"context"
	"testing"
	"time"

	"github.com/gurre/shardcoordinator/dynamolock"
)

func TestIntegration_SingleCoordinatorBecomesLeader(t *testing.T) {
	mockDB := dynamolock.NewTestMockClient()
	locker, err := dynamolock.New(dynamolock.Config{
		Table:  "test-table",
		Client: mockDB,
	})
	if err != nil {
		t.Fatalf("Failed to create locker: %v", err)
	}

	coord, err := New(Config{
		ShardID:       "test-shard",
		OwnerID:       "worker-1",
		LeaseDuration: 200 * time.Millisecond,
		RenewPeriod:   50 * time.Millisecond,
		Locker:        locker,
	})
	if err != nil {
		t.Fatalf("Failed to create coordinator: %v", err)
	}

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	if err := coord.Start(ctx); err != nil {
		t.Fatalf("Failed to start coordinator: %v", err)
	}

	// Wait for coordinator to become leader
	timeout := time.After(500 * time.Millisecond)
	ticker := time.NewTicker(10 * time.Millisecond)
	defer ticker.Stop()

	for {
		select {
		case <-timeout:
			t.Fatal("Coordinator did not become leader within timeout")
		case <-ticker.C:
			if coord.IsLeader() {
				t.Logf("Coordinator became leader")
				return
			}
		}
	}
}

func TestIntegration_TwoCoordinatorsCompete(t *testing.T) {
	mockDB := dynamolock.NewTestMockClient()
	locker, err := dynamolock.New(dynamolock.Config{
		Table:  "test-table",
		Client: mockDB,
	})
	if err != nil {
		t.Fatalf("Failed to create locker: %v", err)
	}

	coord1, err := New(Config{
		ShardID:       "test-shard",
		OwnerID:       "worker-1",
		LeaseDuration: 200 * time.Millisecond,
		RenewPeriod:   50 * time.Millisecond,
		Locker:        locker,
	})
	if err != nil {
		t.Fatalf("Failed to create coordinator 1: %v", err)
	}

	coord2, err := New(Config{
		ShardID:       "test-shard",
		OwnerID:       "worker-2",
		LeaseDuration: 200 * time.Millisecond,
		RenewPeriod:   50 * time.Millisecond,
		Locker:        locker,
	})
	if err != nil {
		t.Fatalf("Failed to create coordinator 2: %v", err)
	}

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	if err := coord1.Start(ctx); err != nil {
		t.Fatalf("Failed to start coordinator 1: %v", err)
	}
	if err := coord2.Start(ctx); err != nil {
		t.Fatalf("Failed to start coordinator 2: %v", err)
	}

	// Wait for one coordinator to become leader
	timeout := time.After(500 * time.Millisecond)
	ticker := time.NewTicker(10 * time.Millisecond)
	defer ticker.Stop()

	for {
		select {
		case <-timeout:
			t.Fatal("No coordinator became leader within timeout")
		case <-ticker.C:
			leader1 := coord1.IsLeader()
			leader2 := coord2.IsLeader()

			if leader1 && leader2 {
				t.Fatal("Both coordinators think they are leader!")
			}

			if leader1 {
				t.Log("Coordinator 1 is leader")
				return
			}
			if leader2 {
				t.Log("Coordinator 2 is leader")
				return
			}
		}
	}
}

func TestIntegration_LeadershipTransfer(t *testing.T) {
	mockDB := dynamolock.NewTestMockClient()
	locker, err := dynamolock.New(dynamolock.Config{
		Table:  "test-table",
		Client: mockDB,
	})
	if err != nil {
		t.Fatalf("Failed to create locker: %v", err)
	}

	coord1, err := New(Config{
		ShardID:       "test-shard",
		OwnerID:       "worker-1",
		LeaseDuration: 200 * time.Millisecond,
		RenewPeriod:   50 * time.Millisecond,
		Locker:        locker,
	})
	if err != nil {
		t.Fatalf("Failed to create coordinator 1: %v", err)
	}

	coord2, err := New(Config{
		ShardID:       "test-shard",
		OwnerID:       "worker-2",
		LeaseDuration: 200 * time.Millisecond,
		RenewPeriod:   50 * time.Millisecond,
		Locker:        locker,
	})
	if err != nil {
		t.Fatalf("Failed to create coordinator 2: %v", err)
	}

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	// Start first coordinator
	if err := coord1.Start(ctx); err != nil {
		t.Fatalf("Failed to start coordinator 1: %v", err)
	}

	// Wait for coord1 to become leader
	if !waitForLeadership(t, coord1, 500*time.Millisecond) {
		t.Fatal("Coordinator 1 did not become leader")
	}
	t.Log("Coordinator 1 became leader")

	// Start second coordinator
	if err := coord2.Start(ctx); err != nil {
		t.Fatalf("Failed to start coordinator 2: %v", err)
	}

	// Verify coord2 does not become leader while coord1 is running
	time.Sleep(200 * time.Millisecond)
	if coord2.IsLeader() {
		t.Fatal("Coordinator 2 should not be leader while coordinator 1 is running")
	}
	t.Log("Coordinator 2 correctly remains follower")

	// Stop coord1
	if err := coord1.Stop(context.Background()); err != nil {
		t.Fatalf("Failed to stop coordinator 1: %v", err)
	}
	t.Log("Stopped coordinator 1")

	// Wait for coord2 to become leader
	if !waitForLeadership(t, coord2, 500*time.Millisecond) {
		t.Fatal("Coordinator 2 did not take over leadership after coordinator 1 stopped")
	}
	t.Log("Coordinator 2 took over leadership")
}

func waitForLeadership(t *testing.T, coord *Coordinator, timeout time.Duration) bool {
	deadline := time.After(timeout)
	ticker := time.NewTicker(10 * time.Millisecond)
	defer ticker.Stop()

	for {
		select {
		case <-deadline:
			return false
		case <-ticker.C:
			if coord.IsLeader() {
				return true
			}
		}
	}
}
