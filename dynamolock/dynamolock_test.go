package dynamolock

import (
	"context"
	"errors"
	"testing"
	"time"

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
