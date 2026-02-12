// Package route53lock provides a Route53 DNS-based implementation of distributed locking.
package route53lock

import (
	"context"
	"errors"
	"fmt"
	"strconv"
	"strings"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/route53"
	"github.com/aws/aws-sdk-go-v2/service/route53/types"
)

// Route53Client defines the minimal Route53 operations required.
// This interface allows for mocking in tests.
type Route53Client interface {
	ChangeResourceRecordSets(ctx context.Context, params *route53.ChangeResourceRecordSetsInput, optFns ...func(*route53.Options)) (*route53.ChangeResourceRecordSetsOutput, error)
	ListResourceRecordSets(ctx context.Context, params *route53.ListResourceRecordSetsInput, optFns ...func(*route53.Options)) (*route53.ListResourceRecordSetsOutput, error)
}

// Config carries the configuration for Route53-based locking.
type Config struct {
	HostedZoneID string        // Route53 hosted zone ID (e.g., "Z1234567890ABC")
	RecordPrefix string        // DNS prefix for locks (e.g., "lock")
	DomainName   string        // Base domain (e.g., "example.com")
	Client       Route53Client // Route53 client interface
}

// Route53Lock implements distributed locking using Route53 DNS records.
type Route53Lock struct {
	hostedZoneID string
	recordPrefix string
	domainName   string
	client       Route53Client
}

// New creates a new Route53-based locker with the provided configuration.
// It validates the configuration and returns an error if any required fields are missing.
func New(cfg Config) (*Route53Lock, error) {
	if cfg.HostedZoneID == "" {
		return nil, errors.New("HostedZoneID is required")
	}
	if cfg.RecordPrefix == "" {
		return nil, errors.New("RecordPrefix is required")
	}
	if cfg.DomainName == "" {
		return nil, errors.New("DomainName is required")
	}
	if cfg.Client == nil {
		return nil, errors.New("Client is required")
	}
	return &Route53Lock{
		hostedZoneID: cfg.HostedZoneID,
		recordPrefix: cfg.RecordPrefix,
		domainName:   cfg.DomainName,
		client:       cfg.Client,
	}, nil
}

// TryAcquire attempts atomic lock acquisition using Route53 batch operations.
// Returns true if acquired, false if already held by another owner with valid TTL (future expiration).
func (r *Route53Lock) TryAcquire(ctx context.Context, shardID string, ownerID string, ttl time.Time) (bool, error) {
	recordName := r.recordName(shardID)
	newValue := r.recordValue(ownerID, ttl)

	// Read current record state
	existing, err := r.getRecord(ctx, recordName)
	if err != nil {
		return false, err
	}

	// Parse existing record if it exists
	if existing != nil {
		_, existingTTL, err := r.parseRecordValue(existing)
		if err != nil {
			return false, err
		}

		// Check if lock is still valid
		if time.Now().Before(existingTTL) {
			return false, nil
		}
	}

	// Build atomic batch: DELETE old (if exists) + CREATE new
	changes := []types.Change{}

	if existing != nil {
		changes = append(changes, types.Change{
			Action: types.ChangeActionDelete,
			ResourceRecordSet: &types.ResourceRecordSet{
				Name:            aws.String(recordName),
				Type:            types.RRTypeTxt,
				TTL:             aws.Int64(60),
				ResourceRecords: existing.ResourceRecords,
			},
		})
	}

	changes = append(changes, types.Change{
		Action: types.ChangeActionCreate,
		ResourceRecordSet: &types.ResourceRecordSet{
			Name: aws.String(recordName),
			Type: types.RRTypeTxt,
			TTL:  aws.Int64(60),
			ResourceRecords: []types.ResourceRecord{
				{Value: aws.String(newValue)},
			},
		},
	})

	// Execute atomic batch
	_, err = r.client.ChangeResourceRecordSets(ctx, &route53.ChangeResourceRecordSetsInput{
		HostedZoneId: aws.String(r.hostedZoneID),
		ChangeBatch: &types.ChangeBatch{
			Changes: changes,
		},
	})

	if err != nil {
		// Check if it's a conflict (another coordinator won the race)
		if isInvalidChangeBatchError(err) {
			return false, nil
		}
		return false, err
	}

	return true, nil
}

// Renew extends lock TTL if caller is the owner.
// Returns true if renewed, false if ownership changed.
func (r *Route53Lock) Renew(ctx context.Context, shardID string, ownerID string, ttl time.Time) (bool, error) {
	recordName := r.recordName(shardID)
	newValue := r.recordValue(ownerID, ttl)

	// Read current record state
	existing, err := r.getRecord(ctx, recordName)
	if err != nil {
		return false, err
	}

	// Record doesn't exist
	if existing == nil {
		return false, nil
	}

	// Parse existing record
	existingOwner, existingTTL, err := r.parseRecordValue(existing)
	if err != nil {
		return false, err
	}

	// Check ownership
	if existingOwner != ownerID {
		return false, nil
	}

	// Check if lock already expired
	if time.Now().After(existingTTL) {
		return false, nil
	}

	// Build atomic batch: DELETE old + CREATE new with updated TTL
	_, err = r.client.ChangeResourceRecordSets(ctx, &route53.ChangeResourceRecordSetsInput{
		HostedZoneId: aws.String(r.hostedZoneID),
		ChangeBatch: &types.ChangeBatch{
			Changes: []types.Change{
				{
					Action: types.ChangeActionDelete,
					ResourceRecordSet: &types.ResourceRecordSet{
						Name:            aws.String(recordName),
						Type:            types.RRTypeTxt,
						TTL:             aws.Int64(60),
						ResourceRecords: existing.ResourceRecords,
					},
				},
				{
					Action: types.ChangeActionCreate,
					ResourceRecordSet: &types.ResourceRecordSet{
						Name: aws.String(recordName),
						Type: types.RRTypeTxt,
						TTL:  aws.Int64(60),
						ResourceRecords: []types.ResourceRecord{
							{Value: aws.String(newValue)},
						},
					},
				},
			},
		},
	})

	if err != nil {
		// Check if it's a conflict (record changed between read and write)
		if isInvalidChangeBatchError(err) {
			return false, nil
		}
		return false, err
	}

	return true, nil
}

// Release deletes lock if caller is the owner.
// Returns error only for infrastructure failures.
func (r *Route53Lock) Release(ctx context.Context, shardID string, ownerID string) error {
	recordName := r.recordName(shardID)

	// Read current record state
	existing, err := r.getRecord(ctx, recordName)
	if err != nil {
		return err
	}

	// Record doesn't exist
	if existing == nil {
		return nil
	}

	// Parse existing record
	existingOwner, _, err := r.parseRecordValue(existing)
	if err != nil {
		return err
	}

	// Only delete if we're the owner
	if existingOwner != ownerID {
		return nil
	}

	// Delete the record
	_, err = r.client.ChangeResourceRecordSets(ctx, &route53.ChangeResourceRecordSetsInput{
		HostedZoneId: aws.String(r.hostedZoneID),
		ChangeBatch: &types.ChangeBatch{
			Changes: []types.Change{
				{
					Action: types.ChangeActionDelete,
					ResourceRecordSet: &types.ResourceRecordSet{
						Name:            aws.String(recordName),
						Type:            types.RRTypeTxt,
						TTL:             aws.Int64(60),
						ResourceRecords: existing.ResourceRecords,
					},
				},
			},
		},
	})

	if err != nil {
		// Ignore conflicts (record changed, ownership changed)
		if isInvalidChangeBatchError(err) {
			return nil
		}
		return err
	}

	return nil
}

// recordName constructs the DNS record name for a shard lock.
func (r *Route53Lock) recordName(shardID string) string {
	return fmt.Sprintf("%s.%s.%s", r.recordPrefix, shardID, r.domainName)
}

// recordValue constructs the TXT record value as space-separated "ownerID unixTimestamp".
func (r *Route53Lock) recordValue(ownerID string, ttl time.Time) string {
	return fmt.Sprintf("\"%s %d\"", ownerID, ttl.Unix())
}

// getRecord retrieves the current TXT record for the given name.
func (r *Route53Lock) getRecord(ctx context.Context, recordName string) (*types.ResourceRecordSet, error) {
	resp, err := r.client.ListResourceRecordSets(ctx, &route53.ListResourceRecordSetsInput{
		HostedZoneId:    aws.String(r.hostedZoneID),
		StartRecordName: aws.String(recordName),
		StartRecordType: types.RRTypeTxt,
		MaxItems:        aws.Int32(1),
	})

	if err != nil {
		return nil, err
	}

	// Check if we found the exact record
	for _, record := range resp.ResourceRecordSets {
		if record.Name != nil && *record.Name == recordName+"." && record.Type == types.RRTypeTxt {
			return &record, nil
		}
	}

	return nil, nil
}

// parseRecordValue extracts owner and TTL from a TXT record.
func (r *Route53Lock) parseRecordValue(record *types.ResourceRecordSet) (string, time.Time, error) {
	if record == nil || len(record.ResourceRecords) == 0 {
		return "", time.Time{}, errors.New("empty record")
	}

	value := *record.ResourceRecords[0].Value
	// Remove quotes from TXT record
	value = strings.Trim(value, "\"")

	parts := strings.SplitN(value, " ", 2)
	if len(parts) != 2 {
		return "", time.Time{}, errors.New("invalid record format")
	}

	ownerID := parts[0]
	ttlUnix, err := strconv.ParseInt(parts[1], 10, 64)
	if err != nil {
		return "", time.Time{}, fmt.Errorf("invalid TTL: %w", err)
	}

	return ownerID, time.Unix(ttlUnix, 0), nil
}

// isInvalidChangeBatchError checks if an error is an InvalidChangeBatch error.
func isInvalidChangeBatchError(err error) bool {
	if err == nil {
		return false
	}
	var icb *types.InvalidChangeBatch
	return errors.As(err, &icb)
}
