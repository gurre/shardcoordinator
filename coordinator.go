// Package shardcoordinator provides distributed leader election for logical shards.
// It implements a fault-tolerant leader election mechanism using DynamoDB as a coordination backend.
// The implementation uses a lease-based approach where a coordinator continually renews its leadership
// to maintain control of a shard.
//
// This coordinator is ideal for scenarios where:
// - You need a single coordinator for a logical shard/task/resource
// - Multiple instances need to cooperate to ensure only one is active
// - You need automatic failover if the leader becomes unavailable
// - You're already using AWS and can leverage DynamoDB
//
// The single-table DynamoDB design uses (pk, sk) without GSIs or scans for efficiency.
//
// DURATION CONFIGURATION IMPACT ON SYSTEM BEHAVIOR:
//
// 1. LeadershipStability: Stability is primarily controlled by LeaseDuration and RenewPeriod.
//   - Longer LeaseDuration increases stability but slows failover
//   - Shorter LeaseDuration speeds up failover but may cause unnecessary transitions
//   - The ideal ratio is 3:1 or 4:1 (LeaseDuration:RenewPeriod)
//
// 2. SplitBrainRisk: Split brain can occur when multiple nodes believe they are leaders.
//   - Risk increases with longer LeaseDuration during network partitions
//   - Risk increases as RenewPeriod approaches LeaseDuration
//   - Short network partitions followed by quick recovery can cause brief split brains
//   - DynamoDB consistency guarantees help mitigate this risk
//
// 3. LeaderlessPeriodsRisk: The system might temporarily have no active leader.
//   - Too short LeaseDuration can cause leadership churn during minor delays
//   - Higher node failure rate increases chance of leaderless periods
//   - RenewPeriod should be short enough to allow multiple renewal attempts
//
// 4. Resilience to network delays:
//   - The system can tolerate network delays up to: LeaseDuration - RenewPeriod
//   - With default 15s lease and 5s renew, it can handle delays up to 10 seconds
//
// RECOMMENDED SETTINGS:
// - For most systems: LeaseDuration=15s, RenewPeriod=5s
// - High stability needs: LeaseDuration=30s, RenewPeriod=10s
// - Fast failover needs: LeaseDuration=10s, RenewPeriod=3s
//
// A good starting point is a 3:1 ratio between LeaseDuration and RenewPeriod,
// which balances stability, split-brain risk, and failover speed.
//
// DESIGN PROPOSAL: DYNAMIC SHARD MANAGEMENT (NOT IMPLEMENTED and needs more thought)
//
// This implementation could be extended to support dynamic shard management with a
// hierarchical coordination model similar to the flow diagram:
//
// 1. Primary Coordinator Extension
//   - Introduce a "MetaCoordinator" that manages overall shard discovery and assignment
//   - Only one worker becomes the MetaCoordinator leader using the existing mechanism
//   - The MetaCoordinator periodically discovers all available shards
//   - It updates a shared registry in DynamoDB with shard metadata
//
// 2. Lease-Based Shard Assignment
//   - Create a "LeaseManager" component that allows workers to acquire leases for shards
//   - Each worker can acquire multiple shard leases up to configured capacity
//   - Leases are stored in DynamoDB with worker ID and expiration (ttl)
//   - Workers periodically renew leases for shards they're processing
//   - If a worker fails, its leases expire and other workers can acquire them
//
// 3. Implementation Changes Needed:
//
//   - Extend the key schema: "SHARD#{shardType}#{shardID}" to support categories
//
//   - Add ListShards() method to retrieve all active shards from DynamoDB
//
//   - Add TakeLease(shardID) method to acquire processing rights to a specific shard
//
//   - Add RelinquishLease(shardID) method for graceful handover
//
//   - Add GetAssignedShards() to retrieve all shards currently assigned to this worker
//
//   - Create ShardConsumerManager to maintain active processing threads
//
//     4. Lease-based coordination algorithm:
//     a. Workers periodically invoke LeaseManager.syncShards()
//     b. Worker checks how many leases it currently holds
//     c. If below target capacity, attempts to acquire more leases with TakeLease()
//     d. If above target capacity, calls RelinquishLease() on some shards
//     e. For all leases held, periodically calls RenewLease()
//     f. LeaseManager maintains a local map of currently assigned shards
//
// 5. Scheduling & Processing:
//   - For each assigned shard, create or reuse a ShardConsumer
//   - ShardConsumer runs in a dedicated thread via ExecutorService
//   - When a lease is lost or relinquished, shutdown the corresponding ShardConsumer
//
// 6. Load Balancing:
//   - Implement a "steal" mechanism where underutilized workers can take leases
//     from overloaded workers (with proper coordination)
//   - Track processing metrics per shard to inform balancing decisions
//   - Use consistent hashing for initial assignment preferences
//
// This extension would allow the system to dynamically adjust to changing shard counts
// without restarting coordinators, while maintaining the fault-tolerance properties
// of the current implementation.
package shardcoordinator

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb/types"
	log "github.com/sirupsen/logrus"
)

// CoordinatorDynamoClient defines the minimal DynamoDB operations required.
// This interface allows for mocking in tests.
type CoordinatorDynamoClient interface {
	PutItem(ctx context.Context, params *dynamodb.PutItemInput, optFns ...func(*dynamodb.Options)) (*dynamodb.PutItemOutput, error)
	DeleteItem(ctx context.Context, params *dynamodb.DeleteItemInput, optFns ...func(*dynamodb.Options)) (*dynamodb.DeleteItemOutput, error)
	UpdateItem(ctx context.Context, params *dynamodb.UpdateItemInput, optFns ...func(*dynamodb.Options)) (*dynamodb.UpdateItemOutput, error)
}

// Coordinator manages the leader election process for a logical shard.
// It maintains leadership by periodically renewing a lease in DynamoDB.
// Only one coordinator can be the leader for a given shard at any time.
//
// Example usage:
//
//	// Create a coordinator for a specific shard
//	cfg := shardcoordinator.Config{
//	    Table:         "coordination-table",
//	    ShardID:       "analytics-task",
//	    OwnerID:       "worker-a937bc",
//	    LeaseDuration: 30 * time.Second,
//	    RenewPeriod:   10 * time.Second,
//	    AWS:           awsConfig,
//	}
//
//	coordinator, err := shardcoordinator.New(cfg)
//	if err != nil {
//	    log.Fatalf("Failed to create coordinator: %v", err)
//	}
//
//	// Start the coordination process
//	ctx := context.Background()
//	if err := coordinator.Start(ctx); err != nil {
//	    log.Fatalf("Failed to start coordinator: %v", err)
//	}
//
//	// Check leadership periodically and run leader-specific tasks
//	for {
//	    if coordinator.IsLeader() {
//	        // Do work that only the leader should do
//	        processAnalyticsBatch()
//	    } else {
//	        // Optional: do follower-specific work or just wait
//	        time.Sleep(1 * time.Second)
//	    }
//	}
//
//	// When shutting down, release leadership gracefully
//	coordinator.Stop(ctx)
type Coordinator struct {
	cfg     Config
	db      CoordinatorDynamoClient
	state   role
	stateMu sync.RWMutex
	stop    context.CancelFunc
}

// Config carries everything the ShardCoordinator needs to run.
// All fields are required except AWS, which can be omitted when using
// a custom DynamoDB client.
type Config struct {
	Table   string // DynamoDB table name (single-table design with pk/sk keys)
	ShardID string // Logical shard/group this coordinator will guard, e.g. "analytics"
	OwnerID string // Unique worker ID, e.g. hostname-PID-UUID. Used as lock owner.

	// LeaseDuration determines how long a coordinator's leadership claim remains valid without renewal.
	// Critical impact on system behavior:
	// - LONGER values provide more stability but slower failover during outages
	// - SHORTER values enable faster leader failover but may cause unnecessary transitions
	// - Network partitions lasting longer than LeaseDuration can lead to split brain scenarios
	// - If set too short, it can cause leadership churn during minor network delays
	// Typical range: 15-60 seconds. Default recommendation: 15 seconds.
	LeaseDuration time.Duration

	// RenewPeriod defines how frequently a leader attempts to renew its lease.
	// Critical impact on system behavior:
	// - MUST be less than LeaseDuration (enforced in New())
	// - Recommended ratio: LeaseDuration should be 3-4× the RenewPeriod
	// - SHORTER values provide more renewal attempts, increasing resilience to intermittent issues
	// - The difference (LeaseDuration - RenewPeriod) defines maximum tolerable network delay
	// - As it approaches LeaseDuration, split brain risk increases
	// Typical range: 5-20 seconds. Default recommendation: 5 seconds.
	RenewPeriod time.Duration

	AWS aws.Config // Pre-configured AWS SDK v2 session (region, creds, retry, etc.)
}

// role represents the state of the coordinator: leader or follower.
type role int

const (
	follower role = iota // The coordinator is not the leader for the shard
	leader               // The coordinator is the active leader for the shard
)

// New creates a new Coordinator with the provided configuration.
// It validates the configuration and returns an error if any required fields are missing
// or if the renew period is too long relative to the lease duration.
//
// Example:
//
//	// Generate a unique owner ID using hostname and PID
//	hostname, _ := os.Hostname()
//	ownerID := fmt.Sprintf("%s-%d-%s", hostname, os.Getpid(), uuid.New().String())
//
//	cfg := shardcoordinator.Config{
//	    Table:         "my-coordination-table",
//	    ShardID:       "batch-processor",
//	    OwnerID:       ownerID,
//	    LeaseDuration: 30 * time.Second,
//	    RenewPeriod:   10 * time.Second,
//	    AWS:           awsConfig,
//	}
//
//	coordinator, err := shardcoordinator.New(cfg)
func New(cfg Config) (*Coordinator, error) {
	if cfg.Table == "" || cfg.ShardID == "" || cfg.OwnerID == "" {
		return nil, errors.New("missing mandatory config")
	}
	if cfg.RenewPeriod >= cfg.LeaseDuration {
		return nil, errors.New("RenewPeriod must be < LeaseDuration")
	}
	return &Coordinator{
		cfg:   cfg,
		db:    dynamodb.NewFromConfig(cfg.AWS),
		state: follower,
	}, nil
}

// IsLeader returns true if this coordinator is currently the leader for its shard.
// This is thread-safe and can be called from multiple goroutines.
//
// Example:
//
//	if coordinator.IsLeader() {
//	    // Only the leader should perform this work
//	    processWorkQueue()
//	}
func (c *Coordinator) IsLeader() bool {
	c.stateMu.RLock()
	defer c.stateMu.RUnlock()
	return c.state == leader
}

// IsFollower returns true if this coordinator is not currently the leader for its shard.
// This is thread-safe and can be called from multiple goroutines.
//
// Example:
//
//	if coordinator.IsFollower() {
//	    // Do follower-specific tasks or just wait
//	    prepareForPotentialLeadership()
//	}
func (c *Coordinator) IsFollower() bool {
	c.stateMu.RLock()
	defer c.stateMu.RUnlock()
	return c.state == follower
}

// Start begins the coordination process, attempting to acquire leadership for the shard
// and maintaining it if successful. It runs a background goroutine that handles the
// leader election loop.
//
// Example:
//
//	ctx := context.Background()
//	err := coordinator.Start(ctx)
//	if err != nil {
//	    log.Fatalf("Failed to start coordinator: %v", err)
//	}
//
//	// The coordinator is now running and will attempt to acquire leadership
func (c *Coordinator) Start(ctx context.Context) error {
	ctx, c.stop = context.WithCancel(ctx)
	go c.loop(ctx)
	return nil
}

// Stop gracefully terminates the coordination process. If this coordinator is the leader,
// it attempts to release the lock so another coordinator can immediately acquire it.
//
// Example:
//
//	// Graceful shutdown
//	ctx := context.Background()
//	err := coordinator.Stop(ctx)
//	if err != nil {
//	    log.Printf("Error stopping coordinator: %v", err)
//	}
func (c *Coordinator) Stop(ctx context.Context) error {
	if c.stop != nil {
		c.stop()
	}

	// Check if we're the leader before attempting to release
	c.stateMu.RLock()
	isLeader := c.state == leader
	c.stateMu.RUnlock()

	// Update internal state to follower before releasing
	c.stateMu.Lock()
	c.state = follower
	c.stateMu.Unlock()

	// Only try to delete the lock if we were the leader
	if isLeader {
		// Delete item if it still belongs to us
		_, err := c.db.DeleteItem(ctx, &dynamodb.DeleteItemInput{
			TableName:           &c.cfg.Table,
			Key:                 c.key(),
			ConditionExpression: aws.String("ownerId = :me"),
			ExpressionAttributeValues: map[string]types.AttributeValue{
				":me": &types.AttributeValueMemberS{Value: c.cfg.OwnerID},
			},
		})

		// Only consider it an error if it's not a conditional check failure
		// (which would just mean someone else took leadership already)
		var ccfe *types.ConditionalCheckFailedException
		if err != nil && !errors.As(err, &ccfe) {
			log.WithFields(log.Fields{
				"shard": c.cfg.ShardID,
				"owner": c.cfg.OwnerID,
				"error": err,
			}).Error("Error releasing leadership lock")
			return err
		}
	}

	return nil
}

// loop is the main coordination routine that handles leader election.
// It runs in a separate goroutine and:
// 1. Continuously attempts to acquire leadership when in follower state
// 2. Periodically renews the leadership lease when in leader state
// 3. Handles transitions between follower and leader states
func (c *Coordinator) loop(ctx context.Context) {
	renewTicker := time.NewTicker(c.cfg.RenewPeriod)
	defer renewTicker.Stop()

	// Use a shorter acquisition attempt interval when in follower state
	acquireTicker := time.NewTicker(c.cfg.RenewPeriod / 2)
	defer acquireTicker.Stop()

	for {
		select {
		case <-ctx.Done():
			return

		case <-acquireTicker.C:
			// Only attempt to acquire if we're currently a follower
			c.stateMu.RLock()
			isFollower := c.state == follower
			c.stateMu.RUnlock()

			if isFollower {
				if c.tryAcquire(ctx) {
					c.stateMu.Lock()
					c.state = leader
					c.stateMu.Unlock()
				}
			}

		case <-renewTicker.C:
			// Only attempt to renew if we're currently a leader
			c.stateMu.RLock()
			isLeader := c.state == leader
			c.stateMu.RUnlock()

			if isLeader && !c.renew(ctx) {
				c.stateMu.Lock()
				c.state = follower
				c.stateMu.Unlock()
			}
		}
	}
}

// tryAcquire attempts to claim leadership for the shard.
// It returns true if successful, false otherwise.
func (c *Coordinator) tryAcquire(ctx context.Context) bool {
	ttl := time.Now().Add(c.cfg.LeaseDuration).Unix()
	_, err := c.db.PutItem(ctx, &dynamodb.PutItemInput{
		TableName: &c.cfg.Table,
		Item: map[string]types.AttributeValue{
			"pk":      &types.AttributeValueMemberS{Value: "SHARD#" + c.cfg.ShardID},
			"sk":      &types.AttributeValueMemberS{Value: "LOCK"},
			"ownerId": &types.AttributeValueMemberS{Value: c.cfg.OwnerID},
			"ttl":     &types.AttributeValueMemberN{Value: fmt.Sprint(ttl)},
		},
		// Since 'ttl' is a reserved keyword, we need to use ExpressionAttributeNames
		// to reference it indirectly as #t
		ConditionExpression: aws.String("attribute_not_exists(pk) OR (attribute_exists(pk) AND #t < :now)"),
		ExpressionAttributeNames: map[string]string{
			"#t": "ttl",
		},
		ExpressionAttributeValues: map[string]types.AttributeValue{
			":now": &types.AttributeValueMemberN{Value: fmt.Sprint(time.Now().Unix())},
		},
	})

	// Properly handle conditional check failures using AWS SDK error types
	var ccfe *types.ConditionalCheckFailedException
	if errors.As(err, &ccfe) {
		// This is normal in leader election - silently return false
		return false
	} else if err != nil {
		// Log other errors as they might indicate real problems
		log.WithFields(log.Fields{
			"shard": c.cfg.ShardID,
			"owner": c.cfg.OwnerID,
			"error": err,
		}).Error("Error trying to acquire lock")
		return false
	}

	// Only log successful acquisition
	// log.WithFields(log.Fields{
	// 	"shard": c.cfg.ShardID,
	// 	"owner": c.cfg.OwnerID,
	// 	"ttl":   ttl,
	// }).Info("Successfully acquired leadership")
	return true
}

// renew attempts to extend the leadership lease.
// It returns true if renewal was successful, false if the lease could not be renewed
// (e.g., because ownership was taken by another coordinator).
func (c *Coordinator) renew(ctx context.Context) bool {
	ttl := time.Now().Add(c.cfg.LeaseDuration).Unix()
	_, err := c.db.UpdateItem(ctx, &dynamodb.UpdateItemInput{
		TableName:           &c.cfg.Table,
		Key:                 c.key(),
		UpdateExpression:    aws.String("SET #t = :n"),
		ConditionExpression: aws.String("ownerId = :me AND #t > :now"),
		ExpressionAttributeNames: map[string]string{
			"#t": "ttl",
		},
		ExpressionAttributeValues: map[string]types.AttributeValue{
			":n":   &types.AttributeValueMemberN{Value: fmt.Sprint(ttl)},
			":me":  &types.AttributeValueMemberS{Value: c.cfg.OwnerID},
			":now": &types.AttributeValueMemberN{Value: fmt.Sprint(time.Now().Unix())},
		},
		ReturnValues: types.ReturnValueNone,
	})

	// Properly handle conditional check failures using AWS SDK error types
	var ccfe *types.ConditionalCheckFailedException
	if errors.As(err, &ccfe) {
		// This is expected when another node took leadership or our lease expired
		// Don't log this as it's normal operation
		return false
	} else if err != nil {
		// Log other errors as they might indicate real problems
		log.WithFields(log.Fields{
			"shard": c.cfg.ShardID,
			"owner": c.cfg.OwnerID,
			"error": err,
		}).Error("Error trying to renew lock")
		return false
	}

	// Don't log successful renewals at all to reduce noise
	return true
}

// key returns the DynamoDB primary key components for the shard lock.
func (c *Coordinator) key() map[string]types.AttributeValue {
	return map[string]types.AttributeValue{
		"pk": &types.AttributeValueMemberS{Value: "SHARD#" + c.cfg.ShardID},
		"sk": &types.AttributeValueMemberS{Value: "LOCK"},
	}
}
