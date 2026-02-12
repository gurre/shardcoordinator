package route53lock

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/route53"
	"github.com/aws/aws-sdk-go-v2/service/route53/types"
	"github.com/gurre/shardcoordinator"
)

// mockRoute53Client implements Route53Client for testing
type mockRoute53Client struct {
	mu              sync.Mutex
	records         map[string]*types.ResourceRecordSet
	changeError     error
	listError       error
	failNextChange  bool
	changeCallCount int
}

func newMockRoute53Client() *mockRoute53Client {
	return &mockRoute53Client{
		records: make(map[string]*types.ResourceRecordSet),
	}
}

func (m *mockRoute53Client) ChangeResourceRecordSets(ctx context.Context, params *route53.ChangeResourceRecordSetsInput, optFns ...func(*route53.Options)) (*route53.ChangeResourceRecordSetsOutput, error) {
	m.mu.Lock()
	defer m.mu.Unlock()

	m.changeCallCount++

	if m.changeError != nil {
		return nil, m.changeError
	}

	if m.failNextChange {
		m.failNextChange = false
		return nil, &types.InvalidChangeBatch{Message: aws.String("Conflict")}
	}

	// Process changes in order
	for _, change := range params.ChangeBatch.Changes {
		recordName := *change.ResourceRecordSet.Name
		// Route53 always uses trailing dots for DNS names
		if !strings.HasSuffix(recordName, ".") {
			recordName = recordName + "."
		}

		switch change.Action {
		case types.ChangeActionDelete:
			// Verify record exists and matches exactly
			existing, exists := m.records[recordName]
			if !exists {
				return nil, &types.InvalidChangeBatch{Message: aws.String("Record does not exist")}
			}
			// Check if values match (for atomic operation)
			if len(existing.ResourceRecords) > 0 && len(change.ResourceRecordSet.ResourceRecords) > 0 {
				if *existing.ResourceRecords[0].Value != *change.ResourceRecordSet.ResourceRecords[0].Value {
					return nil, &types.InvalidChangeBatch{Message: aws.String("Record value mismatch")}
				}
			}
			delete(m.records, recordName)

		case types.ChangeActionCreate:
			// Verify record doesn't exist
			if _, exists := m.records[recordName]; exists {
				return nil, &types.InvalidChangeBatch{Message: aws.String("Record already exists")}
			}
			// Store with trailing dot and ensure Name has trailing dot
			recordCopy := *change.ResourceRecordSet
			recordCopy.Name = aws.String(recordName)
			m.records[recordName] = &recordCopy

		case types.ChangeActionUpsert:
			// Store with trailing dot and ensure Name has trailing dot
			recordCopy := *change.ResourceRecordSet
			recordCopy.Name = aws.String(recordName)
			m.records[recordName] = &recordCopy
		}
	}

	return &route53.ChangeResourceRecordSetsOutput{
		ChangeInfo: &types.ChangeInfo{
			Id:     aws.String("change-id"),
			Status: types.ChangeStatusInsync,
		},
	}, nil
}

func (m *mockRoute53Client) ListResourceRecordSets(ctx context.Context, params *route53.ListResourceRecordSetsInput, optFns ...func(*route53.Options)) (*route53.ListResourceRecordSetsOutput, error) {
	m.mu.Lock()
	defer m.mu.Unlock()

	if m.listError != nil {
		return nil, m.listError
	}

	var recordSets []types.ResourceRecordSet

	// If StartRecordName is specified, look for that specific record
	if params.StartRecordName != nil {
		recordName := *params.StartRecordName + "."
		if record, exists := m.records[recordName]; exists {
			recordSets = append(recordSets, *record)
		}
	} else {
		// Return all records
		for _, record := range m.records {
			recordSets = append(recordSets, *record)
		}
	}

	return &route53.ListResourceRecordSetsOutput{
		ResourceRecordSets: recordSets,
	}, nil
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
				HostedZoneID: "Z123",
				RecordPrefix: "lock",
				DomainName:   "example.com",
				Client:       newMockRoute53Client(),
			},
			wantErr: false,
		},
		{
			name: "missing hosted zone ID",
			cfg: Config{
				RecordPrefix: "lock",
				DomainName:   "example.com",
				Client:       newMockRoute53Client(),
			},
			wantErr: true,
		},
		{
			name: "missing record prefix",
			cfg: Config{
				HostedZoneID: "Z123",
				DomainName:   "example.com",
				Client:       newMockRoute53Client(),
			},
			wantErr: true,
		},
		{
			name: "missing domain name",
			cfg: Config{
				HostedZoneID: "Z123",
				RecordPrefix: "lock",
				Client:       newMockRoute53Client(),
			},
			wantErr: true,
		},
		{
			name: "missing client",
			cfg: Config{
				HostedZoneID: "Z123",
				RecordPrefix: "lock",
				DomainName:   "example.com",
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

func TestRoute53Lock_TryAcquire(t *testing.T) {
	tests := []struct {
		name           string
		setupMock      func(*mockRoute53Client)
		expectAcquired bool
		expectError    bool
	}{
		{
			name: "successful acquisition - no existing record",
			setupMock: func(mock *mockRoute53Client) {
				// No existing record
			},
			expectAcquired: true,
			expectError:    false,
		},
		{
			name: "successful acquisition - expired record",
			setupMock: func(mock *mockRoute53Client) {
				// Pre-populate with expired record (Route53 always uses trailing dots)
				recordName := "lock.test-shard.example.com."
				expiredTTL := time.Now().Add(-1 * time.Hour).Unix()
				mock.records[recordName] = &types.ResourceRecordSet{
					Name: aws.String(recordName),
					Type: types.RRTypeTxt,
					TTL:  aws.Int64(60),
					ResourceRecords: []types.ResourceRecord{
						{Value: aws.String(fmt.Sprintf("\"old-owner %d\"", expiredTTL))},
					},
				}
			},
			expectAcquired: true,
			expectError:    false,
		},
		{
			name: "lock already held - valid TTL",
			setupMock: func(mock *mockRoute53Client) {
				// Pre-populate with valid record (Route53 always uses trailing dots)
				recordName := "lock.test-shard.example.com."
				validTTL := time.Now().Add(1 * time.Hour).Unix()
				mock.records[recordName] = &types.ResourceRecordSet{
					Name: aws.String(recordName),
					Type: types.RRTypeTxt,
					TTL:  aws.Int64(60),
					ResourceRecords: []types.ResourceRecord{
						{Value: aws.String(fmt.Sprintf("\"other-owner %d\"", validTTL))},
					},
				}
			},
			expectAcquired: false,
			expectError:    false,
		},
		{
			name: "conflict during batch operation",
			setupMock: func(mock *mockRoute53Client) {
				mock.failNextChange = true
			},
			expectAcquired: false,
			expectError:    false,
		},
		{
			name: "infrastructure error",
			setupMock: func(mock *mockRoute53Client) {
				mock.changeError = errors.New("route53 error")
			},
			expectAcquired: false,
			expectError:    true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			mockClient := newMockRoute53Client()
			if tt.setupMock != nil {
				tt.setupMock(mockClient)
			}

			locker, _ := New(Config{
				HostedZoneID: "Z123",
				RecordPrefix: "lock",
				DomainName:   "example.com",
				Client:       mockClient,
			})

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

func TestRoute53Lock_Renew(t *testing.T) {
	tests := []struct {
		name          string
		setupMock     func(*mockRoute53Client)
		expectRenewed bool
		expectError   bool
	}{
		{
			name: "successful renewal",
			setupMock: func(mock *mockRoute53Client) {
				// Pre-populate with valid record owned by test-owner
				recordName := "lock.test-shard.example.com."
				validTTL := time.Now().Add(1 * time.Hour).Unix()
				mock.records[recordName] = &types.ResourceRecordSet{
					Name: aws.String(recordName),
					Type: types.RRTypeTxt,
					TTL:  aws.Int64(60),
					ResourceRecords: []types.ResourceRecord{
						{Value: aws.String(fmt.Sprintf("\"test-owner %d\"", validTTL))},
					},
				}
			},
			expectRenewed: true,
			expectError:   false,
		},
		{
			name: "ownership changed",
			setupMock: func(mock *mockRoute53Client) {
				// Pre-populate with record owned by different owner
				recordName := "lock.test-shard.example.com."
				validTTL := time.Now().Add(1 * time.Hour).Unix()
				mock.records[recordName] = &types.ResourceRecordSet{
					Name: aws.String(recordName),
					Type: types.RRTypeTxt,
					TTL:  aws.Int64(60),
					ResourceRecords: []types.ResourceRecord{
						{Value: aws.String(fmt.Sprintf("\"other-owner %d\"", validTTL))},
					},
				}
			},
			expectRenewed: false,
			expectError:   false,
		},
		{
			name: "lock expired",
			setupMock: func(mock *mockRoute53Client) {
				// Pre-populate with expired record
				recordName := "lock.test-shard.example.com."
				expiredTTL := time.Now().Add(-1 * time.Hour).Unix()
				mock.records[recordName] = &types.ResourceRecordSet{
					Name: aws.String(recordName),
					Type: types.RRTypeTxt,
					TTL:  aws.Int64(60),
					ResourceRecords: []types.ResourceRecord{
						{Value: aws.String(fmt.Sprintf("\"test-owner %d\"", expiredTTL))},
					},
				}
			},
			expectRenewed: false,
			expectError:   false,
		},
		{
			name: "record does not exist",
			setupMock: func(mock *mockRoute53Client) {
				// No record
			},
			expectRenewed: false,
			expectError:   false,
		},
		{
			name: "infrastructure error",
			setupMock: func(mock *mockRoute53Client) {
				recordName := "lock.test-shard.example.com."
				validTTL := time.Now().Add(1 * time.Hour).Unix()
				mock.records[recordName] = &types.ResourceRecordSet{
					Name: aws.String(recordName),
					Type: types.RRTypeTxt,
					TTL:  aws.Int64(60),
					ResourceRecords: []types.ResourceRecord{
						{Value: aws.String(fmt.Sprintf("\"test-owner %d\"", validTTL))},
					},
				}
				mock.changeError = errors.New("route53 error")
			},
			expectRenewed: false,
			expectError:   true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			mockClient := newMockRoute53Client()
			if tt.setupMock != nil {
				tt.setupMock(mockClient)
			}

			locker, _ := New(Config{
				HostedZoneID: "Z123",
				RecordPrefix: "lock",
				DomainName:   "example.com",
				Client:       mockClient,
			})

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

func TestRoute53Lock_Release(t *testing.T) {
	tests := []struct {
		name        string
		setupMock   func(*mockRoute53Client)
		expectError bool
	}{
		{
			name: "successful release by owner",
			setupMock: func(mock *mockRoute53Client) {
				// Pre-populate with record owned by test-owner
				recordName := "lock.test-shard.example.com."
				validTTL := time.Now().Add(1 * time.Hour).Unix()
				mock.records[recordName] = &types.ResourceRecordSet{
					Name: aws.String(recordName),
					Type: types.RRTypeTxt,
					TTL:  aws.Int64(60),
					ResourceRecords: []types.ResourceRecord{
						{Value: aws.String(fmt.Sprintf("\"test-owner %d\"", validTTL))},
					},
				}
			},
			expectError: false,
		},
		{
			name: "release by non-owner - no error",
			setupMock: func(mock *mockRoute53Client) {
				// Pre-populate with record owned by different owner
				recordName := "lock.test-shard.example.com."
				validTTL := time.Now().Add(1 * time.Hour).Unix()
				mock.records[recordName] = &types.ResourceRecordSet{
					Name: aws.String(recordName),
					Type: types.RRTypeTxt,
					TTL:  aws.Int64(60),
					ResourceRecords: []types.ResourceRecord{
						{Value: aws.String(fmt.Sprintf("\"other-owner %d\"", validTTL))},
					},
				}
			},
			expectError: false,
		},
		{
			name: "record does not exist - no error",
			setupMock: func(mock *mockRoute53Client) {
				// No record
			},
			expectError: false,
		},
		{
			name: "infrastructure error",
			setupMock: func(mock *mockRoute53Client) {
				recordName := "lock.test-shard.example.com."
				validTTL := time.Now().Add(1 * time.Hour).Unix()
				mock.records[recordName] = &types.ResourceRecordSet{
					Name: aws.String(recordName),
					Type: types.RRTypeTxt,
					TTL:  aws.Int64(60),
					ResourceRecords: []types.ResourceRecord{
						{Value: aws.String(fmt.Sprintf("\"test-owner %d\"", validTTL))},
					},
				}
				mock.changeError = errors.New("route53 error")
			},
			expectError: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			mockClient := newMockRoute53Client()
			if tt.setupMock != nil {
				tt.setupMock(mockClient)
			}

			locker, _ := New(Config{
				HostedZoneID: "Z123",
				RecordPrefix: "lock",
				DomainName:   "example.com",
				Client:       mockClient,
			})

			ctx := context.Background()
			err := locker.Release(ctx, "test-shard", "test-owner")

			if (err != nil) != tt.expectError {
				t.Errorf("Release() error = %v, expectError %v", err, tt.expectError)
			}
		})
	}
}

func TestRoute53Lock_AtomicBatchBehavior(t *testing.T) {
	mockClient := newMockRoute53Client()
	locker, _ := New(Config{
		HostedZoneID: "Z123",
		RecordPrefix: "lock",
		DomainName:   "example.com",
		Client:       mockClient,
	})

	ctx := context.Background()
	recordName := "lock.test-shard.example.com."

	// Create initial lock
	ttl1 := time.Now().Add(1 * time.Hour)
	acquired, err := locker.TryAcquire(ctx, "test-shard", "owner-1", ttl1)
	if !acquired || err != nil {
		t.Fatalf("Failed to acquire initial lock: acquired=%v, err=%v", acquired, err)
	}

	// Simulate another coordinator reading the same record (race setup)
	mockClient.mu.Lock()
	originalRecord := mockClient.records[recordName]
	// Create a copy to avoid pointer issues
	originalRecordCopy := &types.ResourceRecordSet{
		Name:            originalRecord.Name,
		Type:            originalRecord.Type,
		TTL:             originalRecord.TTL,
		ResourceRecords: make([]types.ResourceRecord, len(originalRecord.ResourceRecords)),
	}
	copy(originalRecordCopy.ResourceRecords, originalRecord.ResourceRecords)
	mockClient.mu.Unlock()

	// First coordinator renews (this should succeed and change the record value)
	ttl2 := time.Now().Add(2 * time.Hour)
	renewed, err := locker.Renew(ctx, "test-shard", "owner-1", ttl2)
	if !renewed || err != nil {
		t.Fatalf("Failed to renew lock: renewed=%v, err=%v", renewed, err)
	}

	// Verify the record was updated
	mockClient.mu.Lock()
	currentRecord := mockClient.records[recordName]
	if len(currentRecord.ResourceRecords) == 0 || len(originalRecordCopy.ResourceRecords) == 0 {
		t.Fatal("Missing resource records")
	}
	if *currentRecord.ResourceRecords[0].Value == *originalRecordCopy.ResourceRecords[0].Value {
		t.Fatal("Record should have been updated by Renew")
	}
	mockClient.mu.Unlock()

	// Second coordinator tries to use stale value in atomic DELETE+CREATE (should fail)
	_, err = mockClient.ChangeResourceRecordSets(ctx, &route53.ChangeResourceRecordSetsInput{
		HostedZoneId: aws.String("Z123"),
		ChangeBatch: &types.ChangeBatch{
			Changes: []types.Change{
				{
					Action:            types.ChangeActionDelete,
					ResourceRecordSet: originalRecordCopy, // Using stale value
				},
				{
					Action: types.ChangeActionCreate,
					ResourceRecordSet: &types.ResourceRecordSet{
						Name: aws.String(recordName),
						Type: types.RRTypeTxt,
						TTL:  aws.Int64(60),
						ResourceRecords: []types.ResourceRecord{
							{Value: aws.String("\"owner-2 9999999999\"")},
						},
					},
				},
			},
		},
	})

	// The batch operation should fail with InvalidChangeBatch because DELETE has stale value
	if !isInvalidChangeBatchError(err) {
		t.Errorf("Expected InvalidChangeBatch error for stale DELETE, got: %v", err)
	}

	// Verify the lock is still held by owner-1 (the renewal wasn't overwritten)
	mockClient.mu.Lock()
	finalRecord := mockClient.records[recordName]
	mockClient.mu.Unlock()

	if finalRecord == nil {
		t.Fatal("Record should still exist after failed atomic batch")
	}

	// Parse and verify it's still owned by owner-1 with the renewed TTL
	owner, _, err := locker.parseRecordValue(finalRecord)
	if err != nil {
		t.Fatalf("Failed to parse final record: %v", err)
	}
	if owner != "owner-1" {
		t.Errorf("Lock should still be owned by owner-1, got: %s", owner)
	}
}

func TestRoute53Lock_Conformance(t *testing.T) {
	shardcoordinator.LockerConformanceTest(t, func(t *testing.T) shardcoordinator.Locker {
		mockClient := newMockRoute53Client()
		locker, err := New(Config{
			HostedZoneID: "Z123",
			RecordPrefix: "lock",
			DomainName:   "example.com",
			Client:       mockClient,
		})
		if err != nil {
			t.Fatalf("Failed to create locker: %v", err)
		}
		return locker
	})
}
