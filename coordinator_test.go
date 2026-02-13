package shardcoordinator

import (
	"context"
	"fmt"
	"sync"
	"testing"
	"time"

	"github.com/gurre/shardcoordinator/dynamolock"
)

func TestNew(t *testing.T) {
	mockLocker := createTestLocker(t)

	tests := []struct {
		name    string
		cfg     Config
		wantErr bool
	}{
		{
			name: "valid config",
			cfg: Config{
				ShardID:       "test-shard",
				OwnerID:       "test-owner",
				LeaseDuration: 30 * time.Second,
				RenewPeriod:   10 * time.Second,
				Locker:        mockLocker,
			},
			wantErr: false,
		},
		{
			name: "missing shard ID",
			cfg: Config{
				OwnerID:       "test-owner",
				LeaseDuration: 30 * time.Second,
				RenewPeriod:   10 * time.Second,
				Locker:        mockLocker,
			},
			wantErr: true,
		},
		{
			name: "missing owner ID",
			cfg: Config{
				ShardID:       "test-shard",
				LeaseDuration: 30 * time.Second,
				RenewPeriod:   10 * time.Second,
				Locker:        mockLocker,
			},
			wantErr: true,
		},
		{
			name: "missing locker",
			cfg: Config{
				ShardID:       "test-shard",
				OwnerID:       "test-owner",
				LeaseDuration: 30 * time.Second,
				RenewPeriod:   10 * time.Second,
			},
			wantErr: true,
		},
		{
			name: "renew period >= lease duration",
			cfg: Config{
				ShardID:       "test-shard",
				OwnerID:       "test-owner",
				LeaseDuration: 10 * time.Second,
				RenewPeriod:   10 * time.Second,
				Locker:        mockLocker,
			},
			wantErr: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			_, err := New(tt.cfg)
			if (err != nil) != tt.wantErr {
				t.Errorf("New() error = %v, wantErr %v", err, tt.wantErr)
				return
			}
		})
	}
}

func TestCoordinator_IsLeader(t *testing.T) {
	c := &Coordinator{state: leader}
	if !c.IsLeader() {
		t.Error("IsLeader() should return true")
	}

	c.state = follower
	if c.IsLeader() {
		t.Error("IsLeader() should return false")
	}
}

func TestCoordinator_IsFollower(t *testing.T) {
	c := &Coordinator{state: follower}
	if !c.IsFollower() {
		t.Error("IsFollower() should return true")
	}

	c.state = leader
	if c.IsFollower() {
		t.Error("IsFollower() should return false")
	}
}

// createTestLocker is a helper function to create a mock locker for testing
func createTestLocker(t *testing.T) Locker {
	t.Helper()
	mockDB := dynamolock.NewTestMockClient()
	locker, err := dynamolock.New(dynamolock.Config{
		Table:  "test-table",
		Client: mockDB,
	})
	if err != nil {
		t.Fatalf("Failed to create test locker: %v", err)
	}
	return locker
}

// createTestCoordinator is a helper function to create a coordinator for testing
func createTestCoordinator(locker Locker, ownerID string) *Coordinator {
	return &Coordinator{
		cfg: Config{
			ShardID:       "test-shard",
			OwnerID:       ownerID,
			LeaseDuration: 30 * time.Second,
			RenewPeriod:   10 * time.Second,
			Locker:        locker,
		},
		locker:  locker,
		state:   follower,
		stateMu: sync.RWMutex{},
	}
}

func TestCoordinator_Start_Stop(t *testing.T) {
	locker := createTestLocker(t)
	c := createTestCoordinator(locker, "test-owner")

	ctx := context.Background()
	if err := c.Start(ctx); err != nil {
		t.Fatalf("Start() failed: %v", err)
	}

	// Sleep briefly to allow loop to run
	time.Sleep(50 * time.Millisecond)

	if err := c.Stop(ctx); err != nil {
		t.Fatalf("Stop() failed: %v", err)
	}
}

func TestCoordinator_LoopTimers(t *testing.T) {
	// Create a mock locker
	locker := createTestLocker(t)

	// Use shorter durations for faster testing
	shortRenewPeriod := 50 * time.Millisecond
	shortLeaseDuration := 150 * time.Millisecond

	// Create coordinator
	c := &Coordinator{
		cfg: Config{
			ShardID:       "test-shard",
			OwnerID:       "test-owner",
			LeaseDuration: shortLeaseDuration,
			RenewPeriod:   shortRenewPeriod,
			Locker:        locker,
		},
		locker:  locker,
		state:   follower,
		stateMu: sync.RWMutex{},
	}

	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()

	// Start the coordinator's loop
	err := c.Start(ctx)
	if err != nil {
		t.Fatalf("Failed to start coordinator: %v", err)
	}

	// Wait for the coordinator to run through at least a few cycles
	time.Sleep(1 * time.Second)

	// Stop the coordinator
	err = c.Stop(ctx)
	if err != nil {
		t.Fatalf("Failed to stop coordinator: %v", err)
	}

	// Verify coordinator stopped properly
	if c.IsLeader() {
		t.Logf("Note: Coordinator may still be leader if it acquired and held the lock")
	}
}

// TestEndToEnd_MultipleCoordinators tests a complete leadership lifecycle with multiple coordinators
// competing for leadership of the same shard, including leadership transitions.
func TestEndToEnd_MultipleCoordinators(t *testing.T) {
	// Create a shared mock DB
	mockDB := dynamolock.NewTestMockClient()
	locker, err := dynamolock.New(dynamolock.Config{
		Table:  "test-coordination",
		Client: mockDB,
	})
	if err != nil {
		t.Fatalf("Failed to create locker: %v", err)
	}

	// Test configuration
	const (
		numCoordinators = 3
		leaseDuration   = 200 * time.Millisecond
		renewPeriod     = 50 * time.Millisecond
	)

	// Setup coordinators with tracking
	type coordInfo struct {
		coord   *Coordinator
		active  bool
		ownerID string
	}

	coords := make([]coordInfo, numCoordinators)
	var activeMu sync.RWMutex

	// Create the coordinators
	for i := 0; i < numCoordinators; i++ {
		ownerID := fmt.Sprintf("worker-%d", i+1)
		coord, err := New(Config{
			ShardID:       "shared-resource",
			OwnerID:       ownerID,
			LeaseDuration: leaseDuration,
			RenewPeriod:   renewPeriod,
			Locker:        locker,
		})
		if err != nil {
			t.Fatalf("Failed to create coordinator %d: %v", i, err)
		}
		coords[i] = coordInfo{
			coord:   coord,
			active:  true,
			ownerID: ownerID,
		}
	}

	// Track leadership changes
	type leadershipEvent struct {
		timestamp time.Time
		leader    string
	}
	var leaderHistory []leadershipEvent
	var historyMu sync.Mutex

	// Get current leader's owner ID
	getCurrentLeader := func() string {
		activeMu.RLock()
		defer activeMu.RUnlock()

		for i, info := range coords {
			if info.active && info.coord.IsLeader() {
				return coords[i].ownerID
			}
		}
		return ""
	}

	// Record leadership change
	recordLeaderChange := func(newLeader string) {
		historyMu.Lock()
		defer historyMu.Unlock()

		leaderHistory = append(leaderHistory, leadershipEvent{
			timestamp: time.Now(),
			leader:    newLeader,
		})
	}

	// Setup test context
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	// Ensure all coordinators are stopped at test end
	defer func() {
		for i := range coords {
			coords[i].coord.Stop(context.Background())
		}
		time.Sleep(100 * time.Millisecond) // Give goroutines time to exit
	}()

	// Start coordinators with slight delay to avoid race conditions
	for i := range coords {
		if err := coords[i].coord.Start(ctx); err != nil {
			t.Fatalf("Failed to start coordinator %d: %v", i, err)
		}
		time.Sleep(10 * time.Millisecond)
	}

	// Start leadership monitoring
	leaderMonitorDone := make(chan struct{})
	go func() {
		defer close(leaderMonitorDone)

		ticker := time.NewTicker(20 * time.Millisecond)
		defer ticker.Stop()

		var lastLeader string

		for {
			select {
			case <-ctx.Done():
				return
			case <-ticker.C:
				currentLeader := getCurrentLeader()
				if currentLeader != lastLeader {
					recordLeaderChange(currentLeader)
					lastLeader = currentLeader
				}
			}
		}
	}()

	// Wait for initial leader election (with polling to detect early success)
	// Use a longer timeout since coordinator startup and first acquisition can take time
	var initialLeader string
	timeout := time.After(5 * time.Second)
	checkTicker := time.NewTicker(50 * time.Millisecond)
	defer checkTicker.Stop()

	for initialLeader == "" {
		select {
		case <-timeout:
			// Debug info if we fail
			t.Logf("Failed to get leader. Checking coordinator states...")
			for i, info := range coords {
				t.Logf("  Coordinator %d (owner: %s): active=%v, isLeader=%v",
					i, info.ownerID, info.active, info.coord.IsLeader())
			}
			t.Fatalf("No coordinator became leader after startup")
		case <-checkTicker.C:
			initialLeader = getCurrentLeader()
		}
	}

	// Find the initial leader's index
	initialLeaderIdx := -1
	for i, info := range coords {
		if info.ownerID == initialLeader {
			initialLeaderIdx = i
			break
		}
	}
	if initialLeaderIdx == -1 {
		t.Fatalf("Could not find leader index for owner ID: %s", initialLeader)
	}

	t.Logf("Initial leader is coordinator %d (owner: %s)", initialLeaderIdx, initialLeader)

	// Test scenario 1: Stop the leader and verify leadership transfers
	t.Logf("Stopping the leader (coordinator %d)...", initialLeaderIdx)

	// Mark leader as inactive
	activeMu.Lock()
	coords[initialLeaderIdx].active = false
	activeMu.Unlock()

	// Stop the leader (releases the lock via Release())
	if err := coords[initialLeaderIdx].coord.Stop(ctx); err != nil {
		t.Fatalf("Failed to stop leader: %v", err)
	}

	// Wait for new leader election (with polling, longer timeout for reliability)
	var newLeader string
	newLeaderTimeout := time.After(5 * time.Second)
	newLeaderTicker := time.NewTicker(50 * time.Millisecond)
	defer newLeaderTicker.Stop()

	for newLeader == "" {
		select {
		case <-newLeaderTimeout:
			t.Logf("Failed to get new leader. Checking coordinator states...")
			for i, info := range coords {
				t.Logf("  Coordinator %d (owner: %s): active=%v, isLeader=%v",
					i, info.ownerID, info.active, info.coord.IsLeader())
			}
			t.Fatalf("No new leader elected after stopping initial leader")
		case <-newLeaderTicker.C:
			newLeader = getCurrentLeader()
		}
	}

	// Verify new leader is different from stopped leader
	if newLeader == initialLeader {
		t.Fatalf("New leader is same as stopped leader: %s", newLeader)
	}

	// Stop all remaining active coordinators
	activeMu.RLock()
	activeIndices := make([]int, 0, numCoordinators)
	for i, info := range coords {
		if info.active {
			activeIndices = append(activeIndices, i)
		}
	}
	activeMu.RUnlock()

	for _, i := range activeIndices {
		if err := coords[i].coord.Stop(ctx); err != nil {
			t.Logf("Warning: failed to stop coordinator %d: %v", i, err)
		}
	}

	// Wait for monitoring to complete
	cancel()
	<-leaderMonitorDone

	// Analyze leadership history
	historyMu.Lock()
	defer historyMu.Unlock()

	t.Logf("Leadership timeline (%d events):", len(leaderHistory))
	for i, event := range leaderHistory {
		leaderStr := "none"
		if event.leader != "" {
			leaderStr = event.leader
		}
		t.Logf("  %d. %s: Leader = %s", i, event.timestamp.Format(time.StampMilli), leaderStr)
	}

	// Verify we had multiple leadership transitions
	if len(leaderHistory) < 2 {
		t.Fatalf("Expected at least 2 leadership transitions, got %d", len(leaderHistory))
	}
}

// TestOwnerIDValidation tests that invalid OwnerIDs are rejected during coordinator creation
func TestOwnerIDValidation(t *testing.T) {
	mockLocker := createTestLocker(t)

	tests := []struct {
		name    string
		ownerID string
		valid   bool
	}{
		{"valid alphanumeric", "valid-owner-123", true},
		{"valid with dashes", "my-worker-abc-def", true},
		{"valid with underscores", "worker_123_456", true},
		{"valid with single quote", "owner'with'quote", true},
		{"invalid with space", "owner with space", false},
		{"invalid with tab", "owner\twith\ttab", false},
		{"invalid with newline", "owner\nwith\nnewline", false},
		{"invalid with carriage return", "owner\rwith\rCR", false},
		{"invalid with double quote", "owner\"with\"quote", false},
		{"empty string", "", false},
		{"too long", string(make([]byte, 201)), false},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			_, err := New(Config{
				ShardID:       "test-shard",
				OwnerID:       tt.ownerID,
				LeaseDuration: 30 * time.Second,
				RenewPeriod:   10 * time.Second,
				Locker:        mockLocker,
			})

			if tt.valid && err != nil {
				t.Errorf("Expected valid ownerID %q to pass, got error: %v", tt.ownerID, err)
			}
			if !tt.valid && err == nil {
				t.Errorf("Expected invalid ownerID %q to fail, but no error was returned", tt.ownerID)
			}
		})
	}
}

// TestShardIDValidation tests that invalid ShardIDs are rejected during coordinator creation
func TestShardIDValidation(t *testing.T) {
	mockLocker := createTestLocker(t)

	tests := []struct {
		name    string
		shardID string
		valid   bool
	}{
		{"valid alphanumeric", "valid-shard-123", true},
		{"valid with dashes", "my-shard-abc-def", true},
		{"invalid with space", "shard with space", false},
		{"invalid with tab", "shard\twith\ttab", false},
		{"invalid with newline", "shard\nwith\nnewline", false},
		{"invalid with quote", "shard\"with\"quote", false},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			_, err := New(Config{
				ShardID:       tt.shardID,
				OwnerID:       "valid-owner",
				LeaseDuration: 30 * time.Second,
				RenewPeriod:   10 * time.Second,
				Locker:        mockLocker,
			})

			if tt.valid && err != nil {
				t.Errorf("Expected valid shardID %q to pass, got error: %v", tt.shardID, err)
			}
			if !tt.valid && err == nil {
				t.Errorf("Expected invalid shardID %q to fail, but no error was returned", tt.shardID)
			}
		})
	}
}

// TestTimingConfigurationValidation tests lease duration and renew period validation
func TestTimingConfigurationValidation(t *testing.T) {
	mockLocker := createTestLocker(t)

	tests := []struct {
		name          string
		leaseDuration time.Duration
		renewPeriod   time.Duration
		valid         bool
		description   string
	}{
		{
			name:          "valid 3:1 ratio",
			leaseDuration: 15 * time.Second,
			renewPeriod:   5 * time.Second,
			valid:         true,
			description:   "Recommended 3:1 ratio",
		},
		{
			name:          "valid 4:1 ratio",
			leaseDuration: 20 * time.Second,
			renewPeriod:   5 * time.Second,
			valid:         true,
			description:   "Conservative 4:1 ratio",
		},
		{
			name:          "valid 2:1 ratio",
			leaseDuration: 10 * time.Second,
			renewPeriod:   5 * time.Second,
			valid:         true,
			description:   "Aggressive 2:1 ratio",
		},
		{
			name:          "invalid equal durations",
			leaseDuration: 10 * time.Second,
			renewPeriod:   10 * time.Second,
			valid:         false,
			description:   "RenewPeriod must be less than LeaseDuration",
		},
		{
			name:          "invalid renew period longer",
			leaseDuration: 5 * time.Second,
			renewPeriod:   10 * time.Second,
			valid:         false,
			description:   "RenewPeriod cannot exceed LeaseDuration",
		},
		{
			name:          "valid minimum durations",
			leaseDuration: 2 * time.Second,
			renewPeriod:   1 * time.Second,
			valid:         true,
			description:   "Very short durations but valid ratio",
		},
		{
			name:          "valid long durations",
			leaseDuration: 5 * time.Minute,
			renewPeriod:   1 * time.Minute,
			valid:         true,
			description:   "Long durations for stable systems",
		},
		{
			name:          "edge case renew period one less",
			leaseDuration: 10 * time.Second,
			renewPeriod:   10*time.Second - 1*time.Nanosecond,
			valid:         true,
			description:   "RenewPeriod just under LeaseDuration",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			_, err := New(Config{
				ShardID:       "test-shard",
				OwnerID:       "test-owner",
				LeaseDuration: tt.leaseDuration,
				RenewPeriod:   tt.renewPeriod,
				Locker:        mockLocker,
			})

			if tt.valid && err != nil {
				t.Errorf("%s: Expected valid config to pass, got error: %v", tt.description, err)
			}
			if !tt.valid && err == nil {
				t.Errorf("%s: Expected invalid config to fail, but no error was returned", tt.description)
			}
		})
	}
}

// TestConfigValidation_EdgeCases tests edge cases in coordinator configuration
func TestConfigValidation_EdgeCases(t *testing.T) {
	mockLocker := createTestLocker(t)

	tests := []struct {
		name        string
		cfg         Config
		wantErr     bool
		description string
	}{
		{
			name: "valid minimal config",
			cfg: Config{
				ShardID:       "s",
				OwnerID:       "o",
				LeaseDuration: 2 * time.Second,
				RenewPeriod:   1 * time.Second,
				Locker:        mockLocker,
			},
			wantErr:     false,
			description: "Single character IDs should be valid",
		},
		{
			name: "valid long IDs",
			cfg: Config{
				ShardID:       string(make([]byte, 199)),
				OwnerID:       string(make([]byte, 199)),
				LeaseDuration: 10 * time.Second,
				RenewPeriod:   3 * time.Second,
				Locker:        mockLocker,
			},
			wantErr:     false,
			description: "Long but under limit IDs should be valid",
		},
		{
			name: "invalid nil locker",
			cfg: Config{
				ShardID:       "shard",
				OwnerID:       "owner",
				LeaseDuration: 10 * time.Second,
				RenewPeriod:   3 * time.Second,
				Locker:        nil,
			},
			wantErr:     true,
			description: "Nil locker should be rejected",
		},
		{
			name: "invalid zero lease duration",
			cfg: Config{
				ShardID:       "shard",
				OwnerID:       "owner",
				LeaseDuration: 0,
				RenewPeriod:   1 * time.Second,
				Locker:        mockLocker,
			},
			wantErr:     true,
			description: "Zero lease duration should be rejected",
		},
		{
			name: "invalid zero renew period",
			cfg: Config{
				ShardID:       "shard",
				OwnerID:       "owner",
				LeaseDuration: 10 * time.Second,
				RenewPeriod:   0,
				Locker:        mockLocker,
			},
			wantErr:     true,
			description: "Zero renew period should be rejected",
		},
		{
			name: "invalid negative lease duration",
			cfg: Config{
				ShardID:       "shard",
				OwnerID:       "owner",
				LeaseDuration: -10 * time.Second,
				RenewPeriod:   1 * time.Second,
				Locker:        mockLocker,
			},
			wantErr:     true,
			description: "Negative lease duration should be rejected",
		},
		{
			name: "invalid negative renew period",
			cfg: Config{
				ShardID:       "shard",
				OwnerID:       "owner",
				LeaseDuration: 10 * time.Second,
				RenewPeriod:   -1 * time.Second,
				Locker:        mockLocker,
			},
			wantErr:     true,
			description: "Negative renew period should be rejected",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			// For long IDs, fill with 'a'
			if len(tt.cfg.ShardID) > 1 {
				shardBytes := make([]byte, len(tt.cfg.ShardID))
				for i := range shardBytes {
					shardBytes[i] = 'a'
				}
				tt.cfg.ShardID = string(shardBytes)
			}
			if len(tt.cfg.OwnerID) > 1 {
				ownerBytes := make([]byte, len(tt.cfg.OwnerID))
				for i := range ownerBytes {
					ownerBytes[i] = 'b'
				}
				tt.cfg.OwnerID = string(ownerBytes)
			}

			_, err := New(tt.cfg)

			if (err != nil) != tt.wantErr {
				t.Errorf("%s: New() error = %v, wantErr %v", tt.description, err, tt.wantErr)
			}
		})
	}
}
