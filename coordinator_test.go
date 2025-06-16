package shardcoordinator

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb/types"
)

// mockDynamoClient implements CoordinatorDynamoClient for testing
type mockDynamoClient struct {
	mu              sync.Mutex
	items           map[string]map[string]types.AttributeValue
	putError        error
	updateError     error
	deleteError     error
	conditionFailed bool
}

func newMockDynamoClient() *mockDynamoClient {
	return &mockDynamoClient{
		items: make(map[string]map[string]types.AttributeValue),
	}
}

func (m *mockDynamoClient) PutItem(ctx context.Context, params *dynamodb.PutItemInput, optFns ...func(*dynamodb.Options)) (*dynamodb.PutItemOutput, error) {
	m.mu.Lock()
	defer m.mu.Unlock()

	if m.putError != nil {
		return nil, m.putError
	}

	pk := params.Item["pk"].(*types.AttributeValueMemberS).Value
	sk := params.Item["sk"].(*types.AttributeValueMemberS).Value
	key := pk + "#" + sk

	// Check if item exists and we're configured to fail conditions
	if _, exists := m.items[key]; exists && m.conditionFailed && params.ConditionExpression != nil {
		// With proper condition expressions, this should fail
		return nil, &types.ConditionalCheckFailedException{Message: aws.String("ConditionalCheckFailed")}
	}

	// Store the item
	m.items[key] = params.Item
	return &dynamodb.PutItemOutput{}, nil
}

func (m *mockDynamoClient) DeleteItem(ctx context.Context, params *dynamodb.DeleteItemInput, optFns ...func(*dynamodb.Options)) (*dynamodb.DeleteItemOutput, error) {
	m.mu.Lock()
	defer m.mu.Unlock()

	if m.deleteError != nil {
		return nil, m.deleteError
	}

	pk := params.Key["pk"].(*types.AttributeValueMemberS).Value
	sk := params.Key["sk"].(*types.AttributeValueMemberS).Value
	key := pk + "#" + sk

	// Check conditional expression
	if params.ConditionExpression != nil && *params.ConditionExpression == "ownerId = :me" {
		if _, exists := m.items[key]; !exists {
			return &dynamodb.DeleteItemOutput{}, nil
		}

		ownerID := m.items[key]["ownerId"].(*types.AttributeValueMemberS).Value
		expectedOwnerID := params.ExpressionAttributeValues[":me"].(*types.AttributeValueMemberS).Value

		if ownerID != expectedOwnerID && m.conditionFailed {
			return nil, &types.ConditionalCheckFailedException{Message: aws.String("ConditionalCheckFailed")}
		}
	}

	delete(m.items, key)

	return &dynamodb.DeleteItemOutput{}, nil
}

func (m *mockDynamoClient) UpdateItem(ctx context.Context, params *dynamodb.UpdateItemInput, optFns ...func(*dynamodb.Options)) (*dynamodb.UpdateItemOutput, error) {
	m.mu.Lock()
	defer m.mu.Unlock()

	if m.updateError != nil {
		return nil, m.updateError
	}

	pk := params.Key["pk"].(*types.AttributeValueMemberS).Value
	sk := params.Key["sk"].(*types.AttributeValueMemberS).Value
	key := pk + "#" + sk

	// Check if item exists
	item, exists := m.items[key]
	if !exists {
		return nil, &types.ConditionalCheckFailedException{Message: aws.String("Item does not exist")}
	}

	// If configured to fail conditions and a condition is present, fail the update
	if m.conditionFailed && params.ConditionExpression != nil {
		return nil, &types.ConditionalCheckFailedException{Message: aws.String("ConditionalCheckFailed")}
	}

	// Apply the update
	if params.UpdateExpression != nil && *params.UpdateExpression == "SET #t = :n" {
		newTTL := params.ExpressionAttributeValues[":n"].(*types.AttributeValueMemberN).Value
		item["ttl"] = &types.AttributeValueMemberN{Value: newTTL}
		m.items[key] = item
	}

	return &dynamodb.UpdateItemOutput{}, nil
}

func TestNew(t *testing.T) {
	tests := []struct {
		name    string
		cfg     Config
		wantErr bool
	}{
		{
			name: "valid config",
			cfg: Config{
				Table:         "test-table",
				ShardID:       "test-shard",
				OwnerID:       "test-owner",
				LeaseDuration: 30 * time.Second,
				RenewPeriod:   10 * time.Second,
				AWS:           aws.Config{},
			},
			wantErr: false,
		},
		{
			name: "missing table",
			cfg: Config{
				ShardID:       "test-shard",
				OwnerID:       "test-owner",
				LeaseDuration: 30 * time.Second,
				RenewPeriod:   10 * time.Second,
				AWS:           aws.Config{},
			},
			wantErr: true,
		},
		{
			name: "missing shard ID",
			cfg: Config{
				Table:         "test-table",
				OwnerID:       "test-owner",
				LeaseDuration: 30 * time.Second,
				RenewPeriod:   10 * time.Second,
				AWS:           aws.Config{},
			},
			wantErr: true,
		},
		{
			name: "missing owner ID",
			cfg: Config{
				Table:         "test-table",
				ShardID:       "test-shard",
				LeaseDuration: 30 * time.Second,
				RenewPeriod:   10 * time.Second,
				AWS:           aws.Config{},
			},
			wantErr: true,
		},
		{
			name: "renew period >= lease duration",
			cfg: Config{
				Table:         "test-table",
				ShardID:       "test-shard",
				OwnerID:       "test-owner",
				LeaseDuration: 10 * time.Second,
				RenewPeriod:   10 * time.Second,
				AWS:           aws.Config{},
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

// createTestCoordinator is a helper function to create a coordinator for testing
func createTestCoordinator(db CoordinatorDynamoClient, ownerID string) *Coordinator {
	return &Coordinator{
		cfg: Config{
			Table:         "test-table",
			ShardID:       "test-shard",
			OwnerID:       ownerID,
			LeaseDuration: 30 * time.Second,
			RenewPeriod:   10 * time.Second,
		},
		db:      db,
		state:   follower,
		stateMu: sync.RWMutex{},
	}
}

func TestCoordinator_Start_Stop(t *testing.T) {
	c := createTestCoordinator(newMockDynamoClient(), "test-owner")

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

func TestCoordinator_TryAcquire(t *testing.T) {
	tests := []struct {
		name           string
		setupMock      func(*mockDynamoClient)
		expectAcquired bool
	}{
		{
			name: "successful acquisition",
			setupMock: func(mock *mockDynamoClient) {
				mock.putError = nil
				mock.conditionFailed = false
			},
			expectAcquired: true,
		},
		{
			name: "conditional check failure",
			setupMock: func(mock *mockDynamoClient) {
				mock.conditionFailed = true
				// Pre-populate the item to trigger condition failure
				mock.items["SHARD#test-shard#LOCK"] = map[string]types.AttributeValue{
					"pk":      &types.AttributeValueMemberS{Value: "SHARD#test-shard"},
					"sk":      &types.AttributeValueMemberS{Value: "LOCK"},
					"ownerId": &types.AttributeValueMemberS{Value: "other-owner"},
					"ttl":     &types.AttributeValueMemberN{Value: "9999999999"},
				}
			},
			expectAcquired: false,
		},
		{
			name: "dynamodb error",
			setupMock: func(mock *mockDynamoClient) {
				mock.putError = errors.New("dynamodb error")
			},
			expectAcquired: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			mockDB := newMockDynamoClient()
			if tt.setupMock != nil {
				tt.setupMock(mockDB)
			}

			c := createTestCoordinator(mockDB, "test-owner")
			ctx := context.Background()
			acquired := c.tryAcquire(ctx)

			if acquired != tt.expectAcquired {
				t.Errorf("tryAcquire() = %v, want %v", acquired, tt.expectAcquired)
			}
		})
	}
}

func TestCoordinator_Renew(t *testing.T) {
	tests := []struct {
		name          string
		setupMock     func(*mockDynamoClient)
		expectRenewed bool
	}{
		{
			name: "successful renewal",
			setupMock: func(mock *mockDynamoClient) {
				mock.updateError = nil
				mock.conditionFailed = false
				// Pre-populate the item for renewal
				mock.items["SHARD#test-shard#LOCK"] = map[string]types.AttributeValue{
					"pk":      &types.AttributeValueMemberS{Value: "SHARD#test-shard"},
					"sk":      &types.AttributeValueMemberS{Value: "LOCK"},
					"ownerId": &types.AttributeValueMemberS{Value: "test-owner"},
					"ttl":     &types.AttributeValueMemberN{Value: "9999999999"},
				}
			},
			expectRenewed: true,
		},
		{
			name: "conditional check failure - wrong owner",
			setupMock: func(mock *mockDynamoClient) {
				mock.conditionFailed = true
				// Pre-populate the item with different owner
				mock.items["SHARD#test-shard#LOCK"] = map[string]types.AttributeValue{
					"pk":      &types.AttributeValueMemberS{Value: "SHARD#test-shard"},
					"sk":      &types.AttributeValueMemberS{Value: "LOCK"},
					"ownerId": &types.AttributeValueMemberS{Value: "other-owner"},
					"ttl":     &types.AttributeValueMemberN{Value: "9999999999"},
				}
			},
			expectRenewed: false,
		},
		{
			name: "conditional check failure - expired ttl",
			setupMock: func(mock *mockDynamoClient) {
				mock.conditionFailed = true
				// Pre-populate the item with expired ttl
				mock.items["SHARD#test-shard#LOCK"] = map[string]types.AttributeValue{
					"pk":      &types.AttributeValueMemberS{Value: "SHARD#test-shard"},
					"sk":      &types.AttributeValueMemberS{Value: "LOCK"},
					"ownerId": &types.AttributeValueMemberS{Value: "test-owner"},
					"ttl":     &types.AttributeValueMemberN{Value: "1000"},
				}
			},
			expectRenewed: false,
		},
		{
			name: "dynamodb error",
			setupMock: func(mock *mockDynamoClient) {
				mock.updateError = errors.New("dynamodb error")
				// Pre-populate the item
				mock.items["SHARD#test-shard#LOCK"] = map[string]types.AttributeValue{
					"pk":      &types.AttributeValueMemberS{Value: "SHARD#test-shard"},
					"sk":      &types.AttributeValueMemberS{Value: "LOCK"},
					"ownerId": &types.AttributeValueMemberS{Value: "test-owner"},
					"ttl":     &types.AttributeValueMemberN{Value: "9999999999"},
				}
			},
			expectRenewed: false,
		},
		{
			name: "item does not exist",
			setupMock: func(mock *mockDynamoClient) {
				// Don't pre-populate the item
			},
			expectRenewed: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			mockDB := newMockDynamoClient()
			if tt.setupMock != nil {
				tt.setupMock(mockDB)
			}

			c := createTestCoordinator(mockDB, "test-owner")
			c.state = leader // Set as leader for renewal test
			ctx := context.Background()
			renewed := c.renew(ctx)

			if renewed != tt.expectRenewed {
				t.Errorf("renew() = %v, want %v", renewed, tt.expectRenewed)
			}
		})
	}
}

func TestCoordinator_LoopTimers(t *testing.T) {
	// Create a mock DB with controllable behaviors
	mockDB := newMockDynamoClient()

	// Use shorter durations for faster testing
	shortRenewPeriod := 50 * time.Millisecond
	shortLeaseDuration := 150 * time.Millisecond

	// Create tracking variables - using atomic operations instead of mutex
	var acquireAttempts, renewAttempts int32

	// Create a tracking DB client
	trackedDB := &trackedDynamoClient{
		mockDB: mockDB,
		putItemHook: func() {
			// No mutex needed for this atomic operation
			atomic.AddInt32(&acquireAttempts, 1)
		},
		updateItemHook: func() {
			// Atomic increment of renewAttempts
			currentAttempts := atomic.AddInt32(&renewAttempts, 1)

			// After a few renewals, make them start failing
			// Need to lock mockDB since we're modifying its state
			if currentAttempts >= 3 {
				mockDB.mu.Lock()
				mockDB.conditionFailed = true
				mockDB.mu.Unlock()
			}
		},
	}

	// Create coordinator with tracking DB
	c := &Coordinator{
		cfg: Config{
			Table:         "test-table",
			ShardID:       "test-shard",
			OwnerID:       "test-owner",
			LeaseDuration: shortLeaseDuration,
			RenewPeriod:   shortRenewPeriod,
		},
		db:      trackedDB,
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
	// Leadership should be acquired at first but then lost due to failing renewals
	time.Sleep(1 * time.Second)

	// Now force leadership loss to trigger acquire attempts
	c.stateMu.Lock()
	c.state = follower
	c.stateMu.Unlock()

	// Reset the mock to allow acquisition attempts to be tracked
	mockDB.mu.Lock()
	mockDB.conditionFailed = false
	mockDB.mu.Unlock()

	// Wait for additional cycles to observe acquire attempts
	time.Sleep(1 * time.Second)

	// Stop the coordinator
	err = c.Stop(ctx)
	if err != nil {
		t.Fatalf("Failed to stop coordinator: %v", err)
	}

	// Check results - no mutex needed for atomic reads
	finalAcquireAttempts := atomic.LoadInt32(&acquireAttempts)
	finalRenewAttempts := atomic.LoadInt32(&renewAttempts)

	t.Logf("Acquire attempts: %d, Renew attempts: %d", finalAcquireAttempts, finalRenewAttempts)

	// Verify that acquisition was attempted multiple times
	if finalAcquireAttempts < 2 {
		t.Errorf("Expected multiple acquire attempts, got %d", finalAcquireAttempts)
	}

	// Verify that renewal was attempted multiple times
	if finalRenewAttempts < 3 {
		t.Errorf("Expected multiple renewal attempts, got %d", finalRenewAttempts)
	}

	// Verify final coordinator state reverted to follower
	if c.IsLeader() {
		t.Errorf("Coordinator should have reverted to follower after failed renewals")
	}
}

// trackedDynamoClient wraps the mock client and tracks operations
type trackedDynamoClient struct {
	mockDB         *mockDynamoClient
	putItemHook    func()
	updateItemHook func()
	deleteItemHook func()
}

func (t *trackedDynamoClient) PutItem(ctx context.Context, params *dynamodb.PutItemInput, optFns ...func(*dynamodb.Options)) (*dynamodb.PutItemOutput, error) {
	if t.putItemHook != nil {
		t.putItemHook()
	}
	return t.mockDB.PutItem(ctx, params, optFns...)
}

func (t *trackedDynamoClient) DeleteItem(ctx context.Context, params *dynamodb.DeleteItemInput, optFns ...func(*dynamodb.Options)) (*dynamodb.DeleteItemOutput, error) {
	if t.deleteItemHook != nil {
		t.deleteItemHook()
	}
	return t.mockDB.DeleteItem(ctx, params, optFns...)
}

func (t *trackedDynamoClient) UpdateItem(ctx context.Context, params *dynamodb.UpdateItemInput, optFns ...func(*dynamodb.Options)) (*dynamodb.UpdateItemOutput, error) {
	if t.updateItemHook != nil {
		t.updateItemHook()
	}
	return t.mockDB.UpdateItem(ctx, params, optFns...)
}

// TestEndToEnd_MultipleCoordinators tests a complete leadership lifecycle with multiple coordinators
// competing for leadership of the same shard, including leadership transitions.
func TestEndToEnd_MultipleCoordinators(t *testing.T) {
	// Create a shared mock DB
	mockDB := newMockDynamoClient()

	// Test configuration
	const (
		numCoordinators = 3
		leaseDuration   = 200 * time.Millisecond
		renewPeriod     = 50 * time.Millisecond
		lockKey         = "SHARD#shared-resource#LOCK"
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
		coords[i] = coordInfo{
			coord: &Coordinator{
				cfg: Config{
					Table:         "test-coordination",
					ShardID:       "shared-resource",
					OwnerID:       ownerID,
					LeaseDuration: leaseDuration,
					RenewPeriod:   renewPeriod,
				},
				db:      mockDB,
				state:   follower,
				stateMu: sync.RWMutex{},
			},
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

	// Wait for initial leader election
	time.Sleep(500 * time.Millisecond)

	// Get the initial leader
	initialLeader := getCurrentLeader()
	if initialLeader == "" {
		t.Fatalf("No coordinator became leader after startup")
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

	// Stop the leader
	if err := coords[initialLeaderIdx].coord.Stop(ctx); err != nil {
		t.Fatalf("Failed to stop leader: %v", err)
	}

	// Force clear the lock to speed up failover in test
	mockDB.mu.Lock()
	delete(mockDB.items, lockKey)
	mockDB.mu.Unlock()

	// Wait for new leader election
	time.Sleep(500 * time.Millisecond)

	// Verify new leader was elected
	newLeader := getCurrentLeader()
	if newLeader == "" {
		t.Fatalf("No new leader elected after stopping initial leader")
	}
	if newLeader == initialLeader {
		t.Fatalf("New leader is same as stopped leader: %s", newLeader)
	}

	// Test scenario 2: Simulate network partition
	t.Logf("Simulating network partition...")

	// Force renewal failures
	mockDB.mu.Lock()
	mockDB.conditionFailed = true
	mockDB.mu.Unlock()

	// Wait for leadership to transfer
	time.Sleep(500 * time.Millisecond)

	// Reset conditions and clear lock for clean acquisition
	mockDB.mu.Lock()
	mockDB.conditionFailed = false
	delete(mockDB.items, lockKey)
	mockDB.mu.Unlock()

	// Wait for re-election
	time.Sleep(500 * time.Millisecond)

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
