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

// TestDNSRecordNameConstruction validates that record names are constructed safely
func TestDNSRecordNameConstruction(t *testing.T) {
	tests := []struct {
		name         string
		recordPrefix string
		shardID      string
		domainName   string
		expected     string
	}{
		{
			name:         "standard DNS labels",
			recordPrefix: "lock",
			shardID:      "shard-1",
			domainName:   "example.com",
			expected:     "lock.shard-1.example.com",
		},
		{
			name:         "numeric shard ID",
			recordPrefix: "lock",
			shardID:      "123",
			domainName:   "example.com",
			expected:     "lock.123.example.com",
		},
		{
			name:         "hyphenated labels",
			recordPrefix: "my-lock",
			shardID:      "my-shard-123",
			domainName:   "sub.example.com",
			expected:     "my-lock.my-shard-123.sub.example.com",
		},
		{
			name:         "underscores in labels",
			recordPrefix: "lock_prefix",
			shardID:      "shard_id",
			domainName:   "example.com",
			expected:     "lock_prefix.shard_id.example.com",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			locker, err := New(Config{
				HostedZoneID: "Z123",
				RecordPrefix: tt.recordPrefix,
				DomainName:   tt.domainName,
				Client:       newMockRoute53Client(),
			})
			if err != nil {
				t.Fatalf("New() failed: %v", err)
			}

			result := locker.recordName(tt.shardID)
			if result != tt.expected {
				t.Errorf("recordName() = %q, expected %q", result, tt.expected)
			}
		})
	}
}

// TestRecordValueConstruction validates that TXT record values are formatted correctly
func TestRecordValueConstruction(t *testing.T) {
	locker := &Route53Lock{}

	tests := []struct {
		name     string
		ownerID  string
		ttl      time.Time
		validate func(string) bool
	}{
		{
			name:    "standard owner ID",
			ownerID: "worker-123",
			ttl:     time.Unix(1700000000, 0),
			validate: func(value string) bool {
				return value == "\"worker-123 1700000000\""
			},
		},
		{
			name:    "owner ID with special chars",
			ownerID: "host_name-123-abc",
			ttl:     time.Unix(1700000000, 0),
			validate: func(value string) bool {
				return value == "\"host_name-123-abc 1700000000\""
			},
		},
		{
			name:    "zero timestamp",
			ownerID: "owner",
			ttl:     time.Unix(0, 0),
			validate: func(value string) bool {
				return value == "\"owner 0\""
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := locker.recordValue(tt.ownerID, tt.ttl)
			if !tt.validate(result) {
				t.Errorf("recordValue() = %q, validation failed", result)
			}
		})
	}
}

// TestRecordValueParsing validates parsing of TXT record values with edge cases
func TestRecordValueParsing(t *testing.T) {
	locker := &Route53Lock{}

	tests := []struct {
		name        string
		recordValue string
		expectOwner string
		expectTTL   int64
		expectError bool
	}{
		{
			name:        "valid standard format",
			recordValue: "\"owner-123 1700000000\"",
			expectOwner: "owner-123",
			expectTTL:   1700000000,
			expectError: false,
		},
		{
			name:        "valid with underscores",
			recordValue: "\"worker_abc_123 1700000000\"",
			expectOwner: "worker_abc_123",
			expectTTL:   1700000000,
			expectError: false,
		},
		{
			name:        "valid zero timestamp",
			recordValue: "\"owner 0\"",
			expectOwner: "owner",
			expectTTL:   0,
			expectError: false,
		},
		{
			name:        "valid - missing quotes",
			recordValue: "owner 1700000000",
			expectOwner: "owner",
			expectTTL:   1700000000,
			expectError: false, // Trim handles missing quotes
		},
		{
			name:        "invalid - no space delimiter",
			recordValue: "\"owner1700000000\"",
			expectOwner: "",
			expectTTL:   0,
			expectError: true,
		},
		{
			name:        "invalid - non-numeric timestamp",
			recordValue: "\"owner notanumber\"",
			expectOwner: "",
			expectTTL:   0,
			expectError: true,
		},
		{
			name:        "invalid - empty value",
			recordValue: "\"\"",
			expectOwner: "",
			expectTTL:   0,
			expectError: true,
		},
		{
			name:        "invalid - only owner",
			recordValue: "\"owner\"",
			expectOwner: "",
			expectTTL:   0,
			expectError: true,
		},
		{
			name:        "invalid - extra spaces",
			recordValue: "\"owner  1700000000\"",
			expectOwner: "owner",
			expectTTL:   0,
			expectError: true, // Second part is empty string, can't parse
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			record := &types.ResourceRecordSet{
				ResourceRecords: []types.ResourceRecord{
					{Value: aws.String(tt.recordValue)},
				},
			}

			owner, ttl, err := locker.parseRecordValue(record)

			if (err != nil) != tt.expectError {
				t.Errorf("parseRecordValue() error = %v, expectError %v", err, tt.expectError)
				return
			}

			if !tt.expectError {
				if owner != tt.expectOwner {
					t.Errorf("parseRecordValue() owner = %q, expected %q", owner, tt.expectOwner)
				}
				if ttl.Unix() != tt.expectTTL {
					t.Errorf("parseRecordValue() ttl = %d, expected %d", ttl.Unix(), tt.expectTTL)
				}
			}
		})
	}
}

// TestParseRecordValue_EdgeCases tests edge cases in record value parsing
func TestParseRecordValue_EdgeCases(t *testing.T) {
	locker := &Route53Lock{}

	tests := []struct {
		name        string
		record      *types.ResourceRecordSet
		expectError bool
		errorMsg    string
	}{
		{
			name:        "nil record",
			record:      nil,
			expectError: true,
			errorMsg:    "empty record",
		},
		{
			name: "empty resource records",
			record: &types.ResourceRecordSet{
				ResourceRecords: []types.ResourceRecord{},
			},
			expectError: true,
			errorMsg:    "empty record",
		},
		{
			name: "nil value pointer",
			record: &types.ResourceRecordSet{
				ResourceRecords: []types.ResourceRecord{
					{Value: nil},
				},
			},
			expectError: true,
			errorMsg:    "panic or error",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			defer func() {
				if r := recover(); r != nil && !tt.expectError {
					t.Errorf("parseRecordValue() panicked unexpectedly: %v", r)
				}
			}()

			_, _, err := locker.parseRecordValue(tt.record)

			if (err != nil) != tt.expectError {
				t.Errorf("parseRecordValue() error = %v, expectError %v", err, tt.expectError)
			}
		})
	}
}

// TestErrorDetection validates error classification functions
func TestErrorDetection(t *testing.T) {
	tests := []struct {
		name              string
		err               error
		expectThrottling  bool
		expectRetryable   bool
		expectInvalidBatch bool
	}{
		{
			name:               "throttling exception",
			err:                &types.ThrottlingException{Message: aws.String("Rate exceeded")},
			expectThrottling:   true,
			expectRetryable:    true,
			expectInvalidBatch: false,
		},
		{
			name:               "invalid change batch",
			err:                &types.InvalidChangeBatch{Message: aws.String("Conflict")},
			expectThrottling:   false,
			expectRetryable:    false,
			expectInvalidBatch: true,
		},
		{
			name:               "request timeout",
			err:                errors.New("RequestTimeout: request timed out"),
			expectThrottling:   false,
			expectRetryable:    true,
			expectInvalidBatch: false,
		},
		{
			name:               "service unavailable",
			err:                errors.New("ServiceUnavailable: service is unavailable"),
			expectThrottling:   false,
			expectRetryable:    true,
			expectInvalidBatch: false,
		},
		{
			name:               "internal error",
			err:                errors.New("InternalError: internal server error"),
			expectThrottling:   false,
			expectRetryable:    true,
			expectInvalidBatch: false,
		},
		{
			name:               "other error",
			err:                errors.New("some other error"),
			expectThrottling:   false,
			expectRetryable:    false,
			expectInvalidBatch: false,
		},
		{
			name:               "nil error",
			err:                nil,
			expectThrottling:   false,
			expectRetryable:    false,
			expectInvalidBatch: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if isThrottlingError(tt.err) != tt.expectThrottling {
				t.Errorf("isThrottlingError() = %v, expected %v", isThrottlingError(tt.err), tt.expectThrottling)
			}
			if isRetryableError(tt.err) != tt.expectRetryable {
				t.Errorf("isRetryableError() = %v, expected %v", isRetryableError(tt.err), tt.expectRetryable)
			}
			if isInvalidChangeBatchError(tt.err) != tt.expectInvalidBatch {
				t.Errorf("isInvalidChangeBatchError() = %v, expected %v", isInvalidChangeBatchError(tt.err), tt.expectInvalidBatch)
			}
		})
	}
}

// TestConfigValidation_DNSSafety validates DNS-safe configuration inputs
func TestConfigValidation_DNSSafety(t *testing.T) {
	mockClient := newMockRoute53Client()

	tests := []struct {
		name         string
		hostedZoneID string
		recordPrefix string
		domainName   string
		wantErr      bool
		description  string
	}{
		{
			name:         "valid standard config",
			hostedZoneID: "Z1234567890ABC",
			recordPrefix: "lock",
			domainName:   "example.com",
			wantErr:      false,
			description:  "Standard valid configuration",
		},
		{
			name:         "valid with subdomain",
			hostedZoneID: "Z123",
			recordPrefix: "coordination",
			domainName:   "internal.example.com",
			wantErr:      false,
			description:  "Valid with subdomain",
		},
		{
			name:         "valid with hyphens",
			hostedZoneID: "Z-ABC-123",
			recordPrefix: "my-lock",
			domainName:   "my-domain.com",
			wantErr:      false,
			description:  "Valid with hyphens in labels",
		},
		{
			name:         "valid with underscores",
			hostedZoneID: "Z_123",
			recordPrefix: "lock_prefix",
			domainName:   "example.com",
			wantErr:      false,
			description:  "Valid with underscores (allowed in DNS TXT records)",
		},
		{
			name:         "valid single character prefix",
			hostedZoneID: "Z123",
			recordPrefix: "l",
			domainName:   "example.com",
			wantErr:      false,
			description:  "Valid with single character prefix",
		},
		{
			name:         "valid long labels",
			hostedZoneID: "Z" + strings.Repeat("A", 50),
			recordPrefix: strings.Repeat("a", 63), // DNS label max is 63 chars
			domainName:   "example.com",
			wantErr:      false,
			description:  "Valid with maximum length DNS labels",
		},
		{
			name:         "empty hosted zone",
			hostedZoneID: "",
			recordPrefix: "lock",
			domainName:   "example.com",
			wantErr:      true,
			description:  "Should reject empty hosted zone ID",
		},
		{
			name:         "empty record prefix",
			hostedZoneID: "Z123",
			recordPrefix: "",
			domainName:   "example.com",
			wantErr:      true,
			description:  "Should reject empty record prefix",
		},
		{
			name:         "empty domain name",
			hostedZoneID: "Z123",
			recordPrefix: "lock",
			domainName:   "",
			wantErr:      true,
			description:  "Should reject empty domain name",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			_, err := New(Config{
				HostedZoneID: tt.hostedZoneID,
				RecordPrefix: tt.recordPrefix,
				DomainName:   tt.domainName,
				Client:       mockClient,
			})

			if (err != nil) != tt.wantErr {
				t.Errorf("%s: New() error = %v, wantErr %v", tt.description, err, tt.wantErr)
			}
		})
	}
}
