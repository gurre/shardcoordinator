package dynamolock

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb/types"
	"github.com/gurre/shardcoordinator"
)

func TestNew(t *testing.T) {
	tests := []struct {
		name    string
		cfg     Config
		wantErr bool
	}{
		{
			name: "valid config",
			cfg: Config{
				Table:  "test-table",
				Client: NewMockDynamoClient(),
			},
			wantErr: false,
		},
		{
			name: "missing table",
			cfg: Config{
				Client: NewMockDynamoClient(),
			},
			wantErr: true,
		},
		{
			name: "missing client",
			cfg: Config{
				Table: "test-table",
			},
			wantErr: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			_, err := New(tt.cfg)
			if (err != nil) != tt.wantErr {
				t.Errorf("New() error = %v, wantErr %v", err, tt.wantErr)
			}
		})
	}
}

func TestDynamoLock_TryAcquire(t *testing.T) {
	tests := []struct {
		name           string
		setupMock      func(*MockDynamoClient)
		expectAcquired bool
		expectError    bool
	}{
		{
			name:           "successful acquisition",
			setupMock:      func(mock *MockDynamoClient) {},
			expectAcquired: true,
			expectError:    false,
		},
		{
			name: "infrastructure error",
			setupMock: func(mock *MockDynamoClient) {
				mock.putError = errors.New("dynamodb error")
			},
			expectAcquired: false,
			expectError:    true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			mockDB := NewMockDynamoClient()
			if tt.setupMock != nil {
				tt.setupMock(mockDB)
			}

			locker, _ := New(Config{Table: "test-table", Client: mockDB})
			ctx := context.Background()
			ttl := time.Now().Add(30 * time.Second)
			acquired, err := locker.TryAcquire(ctx, "test-shard", "test-owner", ttl)

			if (err != nil) != tt.expectError {
				t.Errorf("TryAcquire() error = %v, expectError %v", err, tt.expectError)
			}
			if acquired != tt.expectAcquired {
				t.Errorf("TryAcquire() acquired = %v, expectAcquired %v", acquired, tt.expectAcquired)
			}
		})
	}
}

func TestDynamoLock_Renew(t *testing.T) {
	tests := []struct {
		name          string
		setupMock     func(*MockDynamoClient)
		expectRenewed bool
		expectError   bool
	}{
		{
			name: "successful renewal",
			setupMock: func(mock *MockDynamoClient) {
				// First acquire the lock
				locker, _ := New(Config{Table: "test-table", Client: mock})
				ctx := context.Background()
				ttl := time.Now().Add(30 * time.Second)
				locker.TryAcquire(ctx, "test-shard", "test-owner", ttl)
			},
			expectRenewed: true,
			expectError:   false,
		},
		{
			name: "no lock exists",
			setupMock: func(mock *MockDynamoClient) {
				// No lock acquired, so renew fails (item does not exist)
			},
			expectRenewed: false,
			expectError:   false,
		},
		{
			name: "infrastructure error",
			setupMock: func(mock *MockDynamoClient) {
				// First acquire the lock
				locker, _ := New(Config{Table: "test-table", Client: mock})
				ctx := context.Background()
				ttl := time.Now().Add(30 * time.Second)
				locker.TryAcquire(ctx, "test-shard", "test-owner", ttl)
				// Then set error
				mock.updateError = errors.New("dynamodb error")
			},
			expectRenewed: false,
			expectError:   true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			mockDB := NewMockDynamoClient()
			if tt.setupMock != nil {
				tt.setupMock(mockDB)
			}

			locker, _ := New(Config{Table: "test-table", Client: mockDB})
			ctx := context.Background()
			ttl := time.Now().Add(30 * time.Second)
			renewed, err := locker.Renew(ctx, "test-shard", "test-owner", ttl)

			if (err != nil) != tt.expectError {
				t.Errorf("Renew() error = %v, expectError %v", err, tt.expectError)
			}
			if renewed != tt.expectRenewed {
				t.Errorf("Renew() renewed = %v, expectRenewed %v", renewed, tt.expectRenewed)
			}
		})
	}
}

func TestDynamoLock_Release(t *testing.T) {
	tests := []struct {
		name        string
		setupMock   func(*MockDynamoClient)
		expectError bool
	}{
		{
			name: "successful release by owner",
			setupMock: func(mock *MockDynamoClient) {
				// First acquire the lock
				locker, _ := New(Config{Table: "test-table", Client: mock})
				ctx := context.Background()
				ttl := time.Now().Add(30 * time.Second)
				locker.TryAcquire(ctx, "test-shard", "test-owner", ttl)
			},
			expectError: false,
		},
		{
			name: "release by non-owner - no error",
			setupMock: func(mock *MockDynamoClient) {
				// First acquire the lock with different owner
				locker, _ := New(Config{Table: "test-table", Client: mock})
				ctx := context.Background()
				ttl := time.Now().Add(30 * time.Second)
				locker.TryAcquire(ctx, "test-shard", "other-owner", ttl)
			},
			expectError: false,
		},
		{
			name: "infrastructure error",
			setupMock: func(mock *MockDynamoClient) {
				// First acquire the lock
				locker, _ := New(Config{Table: "test-table", Client: mock})
				ctx := context.Background()
				ttl := time.Now().Add(30 * time.Second)
				locker.TryAcquire(ctx, "test-shard", "test-owner", ttl)
				// Then set error
				mock.deleteError = errors.New("dynamodb error")
			},
			expectError: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			mockDB := NewMockDynamoClient()
			if tt.setupMock != nil {
				tt.setupMock(mockDB)
			}

			locker, _ := New(Config{Table: "test-table", Client: mockDB})
			ctx := context.Background()
			err := locker.Release(ctx, "test-shard", "test-owner")

			if (err != nil) != tt.expectError {
				t.Errorf("Release() error = %v, expectError %v", err, tt.expectError)
			}
		})
	}
}

func TestDynamoLock_Conformance(t *testing.T) {
	shardcoordinator.LockerConformanceTest(t, func(t *testing.T) shardcoordinator.Locker {
		mockDB := NewMockDynamoClient()
		locker, err := New(Config{Table: "test-conformance", Client: mockDB})
		if err != nil {
			t.Fatalf("Failed to create locker: %v", err)
		}
		return locker
	})
}

// TestTableNameValidation validates various table name inputs
func TestTableNameValidation(t *testing.T) {
	mockClient := NewMockDynamoClient()

	tests := []struct {
		name      string
		tableName string
		wantErr   bool
	}{
		{
			name:      "valid standard table name",
			tableName: "coordination-table",
			wantErr:   false,
		},
		{
			name:      "valid with underscores",
			tableName: "coordination_table_v2",
			wantErr:   false,
		},
		{
			name:      "valid with dots",
			tableName: "coordination.table",
			wantErr:   false,
		},
		{
			name:      "valid single character",
			tableName: "t",
			wantErr:   false,
		},
		{
			name:      "valid numbers",
			tableName: "table123",
			wantErr:   false,
		},
		{
			name:      "valid long name",
			tableName: "coordination-table-for-distributed-locking-system",
			wantErr:   false,
		},
		{
			name:      "empty table name",
			tableName: "",
			wantErr:   true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			_, err := New(Config{
				Table:  tt.tableName,
				Client: mockClient,
			})

			if (err != nil) != tt.wantErr {
				t.Errorf("New() error = %v, wantErr %v", err, tt.wantErr)
			}
		})
	}
}

// TestKeyConstruction validates that DynamoDB keys are constructed correctly
func TestKeyConstruction(t *testing.T) {
	locker := &DynamoLock{
		table: "test-table",
	}

	tests := []struct {
		name        string
		shardID     string
		expectedPK  string
		expectedSK  string
	}{
		{
			name:       "standard shard ID",
			shardID:    "shard-1",
			expectedPK: "SHARD#shard-1",
			expectedSK: "LOCK",
		},
		{
			name:       "numeric shard ID",
			shardID:    "123",
			expectedPK: "SHARD#123",
			expectedSK: "LOCK",
		},
		{
			name:       "shard ID with underscores",
			shardID:    "shard_id_123",
			expectedPK: "SHARD#shard_id_123",
			expectedSK: "LOCK",
		},
		{
			name:       "shard ID with special chars",
			shardID:    "batch-processor:v2",
			expectedPK: "SHARD#batch-processor:v2",
			expectedSK: "LOCK",
		},
		{
			name:       "empty shard ID",
			shardID:    "",
			expectedPK: "SHARD#",
			expectedSK: "LOCK",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			key := locker.key(tt.shardID)

			pkAttr, ok := key["pk"]
			if !ok {
				t.Fatal("key missing 'pk' attribute")
			}
			pkValue, ok := pkAttr.(*types.AttributeValueMemberS)
			if !ok {
				t.Fatal("pk is not a string attribute")
			}
			if pkValue.Value != tt.expectedPK {
				t.Errorf("pk = %q, expected %q", pkValue.Value, tt.expectedPK)
			}

			skAttr, ok := key["sk"]
			if !ok {
				t.Fatal("key missing 'sk' attribute")
			}
			skValue, ok := skAttr.(*types.AttributeValueMemberS)
			if !ok {
				t.Fatal("sk is not a string attribute")
			}
			if skValue.Value != tt.expectedSK {
				t.Errorf("sk = %q, expected %q", skValue.Value, tt.expectedSK)
			}
		})
	}
}

// TestShardIDOwnerID_SpecialCharacters validates that special characters work correctly
func TestShardIDOwnerID_SpecialCharacters(t *testing.T) {
	tests := []struct {
		name    string
		shardID string
		ownerID string
		wantErr bool
	}{
		{
			name:    "standard identifiers",
			shardID: "shard-1",
			ownerID: "worker-1",
			wantErr: false,
		},
		{
			name:    "identifiers with colons",
			shardID: "namespace:shard:1",
			ownerID: "host:process:123",
			wantErr: false,
		},
		{
			name:    "identifiers with slashes",
			shardID: "region/az/shard",
			ownerID: "dc/rack/host",
			wantErr: false,
		},
		{
			name:    "identifiers with dots",
			shardID: "shard.v2.prod",
			ownerID: "worker.example.com",
			wantErr: false,
		},
		{
			name:    "identifiers with underscores",
			shardID: "shard_id_123",
			ownerID: "worker_abc_def",
			wantErr: false,
		},
		{
			name:    "identifiers with mixed special chars",
			shardID: "app-v2_shard.1:prod",
			ownerID: "host-1_worker.abc:123",
			wantErr: false,
		},
		{
			name:    "unicode identifiers",
			shardID: "shard-日本",
			ownerID: "worker-αβγ",
			wantErr: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			mockDB := NewMockDynamoClient()
			locker, err := New(Config{
				Table:  "test-table",
				Client: mockDB,
			})
			if err != nil {
				t.Fatalf("Failed to create locker: %v", err)
			}

			ctx := context.Background()
			ttl := time.Now().Add(30 * time.Second)

			// Try to acquire lock
			acquired, err := locker.TryAcquire(ctx, tt.shardID, tt.ownerID, ttl)

			if (err != nil) != tt.wantErr {
				t.Errorf("TryAcquire() error = %v, wantErr %v", err, tt.wantErr)
			}

			if !tt.wantErr && !acquired {
				t.Error("TryAcquire() should have succeeded")
			}

			// Try to renew
			if !tt.wantErr && acquired {
				renewed, err := locker.Renew(ctx, tt.shardID, tt.ownerID, ttl)
				if err != nil {
					t.Errorf("Renew() error = %v", err)
				}
				if !renewed {
					t.Error("Renew() should have succeeded")
				}

				// Try to release
				err = locker.Release(ctx, tt.shardID, tt.ownerID)
				if err != nil {
					t.Errorf("Release() error = %v", err)
				}
			}
		})
	}
}

// TestErrorDetection_DynamoDB validates error classification functions
func TestErrorDetection_DynamoDB(t *testing.T) {
	tests := []struct {
		name                      string
		err                       error
		expectThrottling          bool
		expectRetryable           bool
		expectConditionalCheckFail bool
	}{
		{
			name:                       "provisioned throughput exceeded",
			err:                        &types.ProvisionedThroughputExceededException{Message: aws.String("Throughput exceeded")},
			expectThrottling:           true,
			expectRetryable:            true,
			expectConditionalCheckFail: false,
		},
		{
			name:                       "conditional check failed",
			err:                        &types.ConditionalCheckFailedException{Message: aws.String("Condition failed")},
			expectThrottling:           false,
			expectRetryable:            false,
			expectConditionalCheckFail: true,
		},
		{
			name:                       "request timeout",
			err:                        errors.New("RequestTimeout: request timed out"),
			expectThrottling:           false,
			expectRetryable:            true,
			expectConditionalCheckFail: false,
		},
		{
			name:                       "service unavailable",
			err:                        errors.New("ServiceUnavailable: service is unavailable"),
			expectThrottling:           false,
			expectRetryable:            true,
			expectConditionalCheckFail: false,
		},
		{
			name:                       "internal server error",
			err:                        errors.New("InternalServerError: internal error"),
			expectThrottling:           false,
			expectRetryable:            true,
			expectConditionalCheckFail: false,
		},
		{
			name:                       "other error",
			err:                        errors.New("some other error"),
			expectThrottling:           false,
			expectRetryable:            false,
			expectConditionalCheckFail: false,
		},
		{
			name:                       "nil error",
			err:                        nil,
			expectThrottling:           false,
			expectRetryable:            false,
			expectConditionalCheckFail: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if isDynamoThrottlingError(tt.err) != tt.expectThrottling {
				t.Errorf("isDynamoThrottlingError() = %v, expected %v", isDynamoThrottlingError(tt.err), tt.expectThrottling)
			}
			if isRetryableError(tt.err) != tt.expectRetryable {
				t.Errorf("isRetryableError() = %v, expected %v", isRetryableError(tt.err), tt.expectRetryable)
			}

			var ccfe *types.ConditionalCheckFailedException
			if errors.As(tt.err, &ccfe) != tt.expectConditionalCheckFail {
				t.Errorf("ConditionalCheckFailedException detection = %v, expected %v",
					errors.As(tt.err, &ccfe), tt.expectConditionalCheckFail)
			}
		})
	}
}

// TestTTLBehavior validates that TTL expiration is handled correctly
func TestTTLBehavior(t *testing.T) {
	mockDB := NewMockDynamoClient()
	locker, _ := New(Config{
		Table:  "test-table",
		Client: mockDB,
	})

	ctx := context.Background()

	// Acquire lock with short TTL
	pastTTL := time.Now().Add(-1 * time.Hour)
	acquired, err := locker.TryAcquire(ctx, "test-shard", "owner-1", pastTTL)
	if err != nil {
		t.Fatalf("Failed to acquire lock: %v", err)
	}
	if !acquired {
		t.Fatal("Should have acquired lock even with past TTL (creation time)")
	}

	// Different owner should be able to acquire expired lock
	futureTTL := time.Now().Add(1 * time.Hour)
	acquired2, err := locker.TryAcquire(ctx, "test-shard", "owner-2", futureTTL)
	if err != nil {
		t.Fatalf("Failed to acquire expired lock: %v", err)
	}
	if !acquired2 {
		t.Error("Should have acquired expired lock")
	}

	// Verify owner-2 now owns the lock
	renewed, err := locker.Renew(ctx, "test-shard", "owner-2", futureTTL)
	if err != nil {
		t.Fatalf("Failed to renew lock: %v", err)
	}
	if !renewed {
		t.Error("Owner-2 should be able to renew the lock")
	}

	// Verify owner-1 cannot renew (lost ownership)
	renewed1, err := locker.Renew(ctx, "test-shard", "owner-1", futureTTL)
	if err != nil {
		t.Fatalf("Unexpected error: %v", err)
	}
	if renewed1 {
		t.Error("Owner-1 should not be able to renew (lost ownership)")
	}
}
