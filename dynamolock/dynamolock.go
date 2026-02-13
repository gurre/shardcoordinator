// Package dynamolock provides a DynamoDB-based implementation of distributed locking.
//
// # Rate Limits
//
// DynamoDB has high default throughput (~40,000 RCU) but can still throttle under heavy load.
// This implementation includes automatic retry with exponential backoff for throttling errors.
// Consider using auto-scaling or on-demand billing mode for variable workloads.
package dynamolock

import (
	"context"
	"errors"
	"fmt"
	"math/rand"
	"strings"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb/types"
)

// DynamoClient defines the minimal DynamoDB operations required.
// This interface allows for mocking in tests.
type DynamoClient interface {
	PutItem(ctx context.Context, params *dynamodb.PutItemInput, optFns ...func(*dynamodb.Options)) (*dynamodb.PutItemOutput, error)
	DeleteItem(ctx context.Context, params *dynamodb.DeleteItemInput, optFns ...func(*dynamodb.Options)) (*dynamodb.DeleteItemOutput, error)
	UpdateItem(ctx context.Context, params *dynamodb.UpdateItemInput, optFns ...func(*dynamodb.Options)) (*dynamodb.UpdateItemOutput, error)
}

// retryWithBackoff executes a function with exponential backoff on retryable errors.
// It retries up to maxRetries times with exponential backoff and jitter.
// Returns the last error if all retries are exhausted.
func retryWithBackoff(ctx context.Context, maxRetries int, fn func() error, isRetryable func(error) bool) error {
	var lastErr error
	for attempt := 0; attempt < maxRetries; attempt++ {
		lastErr = fn()
		if lastErr == nil {
			return nil
		}
		if !isRetryable(lastErr) {
			return lastErr
		}
		if attempt < maxRetries-1 {
			backoff := time.Duration(1<<attempt) * 100 * time.Millisecond
			jitter := time.Duration(rand.Int63n(int64(backoff / 2)))
			select {
			case <-time.After(backoff + jitter):
			case <-ctx.Done():
				return ctx.Err()
			}
		}
	}
	return lastErr
}

// isDynamoThrottlingError checks if an error is a DynamoDB throttling error.
func isDynamoThrottlingError(err error) bool {
	if err == nil {
		return false
	}
	var pte *types.ProvisionedThroughputExceededException
	return errors.As(err, &pte)
}

// isRetryableError checks if an error should be retried.
func isRetryableError(err error) bool {
	if err == nil {
		return false
	}
	// Retry on throttling
	if isDynamoThrottlingError(err) {
		return true
	}
	// Retry on transient errors
	errStr := err.Error()
	return strings.Contains(errStr, "RequestTimeout") ||
		strings.Contains(errStr, "ServiceUnavailable") ||
		strings.Contains(errStr, "InternalServerError")
}

// Config carries the configuration for DynamoDB-based locking.
type Config struct {
	Table  string       // DynamoDB table name (single-table design with pk/sk keys)
	Client DynamoClient // DynamoDB client interface
}

// DynamoLock implements distributed locking using DynamoDB.
type DynamoLock struct {
	table  string
	client DynamoClient
}

// New creates a new DynamoDB-based locker with the provided configuration.
// It validates the configuration and returns an error if any required fields are missing.
func New(cfg Config) (*DynamoLock, error) {
	if cfg.Table == "" {
		return nil, errors.New("Table is required")
	}
	if cfg.Client == nil {
		return nil, errors.New("Client is required")
	}
	return &DynamoLock{
		table:  cfg.Table,
		client: cfg.Client,
	}, nil
}

// TryAcquire attempts atomic lock acquisition using DynamoDB conditional writes.
// Returns true if acquired, false if already held by another owner with valid TTL (future expiration).
func (d *DynamoLock) TryAcquire(ctx context.Context, shardID string, ownerID string, ttl time.Time) (bool, error) {
	ttlUnix := ttl.Unix()

	var putErr error
	retryErr := retryWithBackoff(ctx, 4, func() error {
		_, putErr = d.client.PutItem(ctx, &dynamodb.PutItemInput{
			TableName: &d.table,
			Item: map[string]types.AttributeValue{
				"pk":      &types.AttributeValueMemberS{Value: "SHARD#" + shardID},
				"sk":      &types.AttributeValueMemberS{Value: "LOCK"},
				"ownerId": &types.AttributeValueMemberS{Value: ownerID},
				"ttl":     &types.AttributeValueMemberN{Value: fmt.Sprint(ttlUnix)},
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
		return putErr
	}, func(err error) bool {
		// Don't retry ConditionalCheckFailedException (application-level lock contention)
		var ccfe *types.ConditionalCheckFailedException
		if errors.As(err, &ccfe) {
			return false
		}
		return isRetryableError(err)
	})

	var ccfe *types.ConditionalCheckFailedException
	if errors.As(putErr, &ccfe) {
		return false, nil
	} else if retryErr != nil {
		return false, retryErr
	}

	return true, nil
}

// Renew extends lock TTL if caller is the owner.
// Returns true if renewed, false if ownership changed.
func (d *DynamoLock) Renew(ctx context.Context, shardID string, ownerID string, ttl time.Time) (bool, error) {
	ttlUnix := ttl.Unix()

	var updateErr error
	retryErr := retryWithBackoff(ctx, 4, func() error {
		_, updateErr = d.client.UpdateItem(ctx, &dynamodb.UpdateItemInput{
			TableName:           &d.table,
			Key:                 d.key(shardID),
			UpdateExpression:    aws.String("SET #t = :n"),
			ConditionExpression: aws.String("ownerId = :me AND #t >= :now"),
			ExpressionAttributeNames: map[string]string{
				"#t": "ttl",
			},
			ExpressionAttributeValues: map[string]types.AttributeValue{
				":n":   &types.AttributeValueMemberN{Value: fmt.Sprint(ttlUnix)},
				":me":  &types.AttributeValueMemberS{Value: ownerID},
				":now": &types.AttributeValueMemberN{Value: fmt.Sprint(time.Now().Unix())},
			},
			ReturnValues: types.ReturnValueNone,
		})
		return updateErr
	}, func(err error) bool {
		// Don't retry ConditionalCheckFailedException (application-level lock contention)
		var ccfe *types.ConditionalCheckFailedException
		if errors.As(err, &ccfe) {
			return false
		}
		return isRetryableError(err)
	})

	var ccfe *types.ConditionalCheckFailedException
	if errors.As(updateErr, &ccfe) {
		return false, nil
	} else if retryErr != nil {
		return false, retryErr
	}

	return true, nil
}

// Release deletes lock if caller is the owner.
// Returns error only for infrastructure failures.
func (d *DynamoLock) Release(ctx context.Context, shardID string, ownerID string) error {
	var deleteErr error
	retryErr := retryWithBackoff(ctx, 4, func() error {
		_, deleteErr = d.client.DeleteItem(ctx, &dynamodb.DeleteItemInput{
			TableName:           &d.table,
			Key:                 d.key(shardID),
			ConditionExpression: aws.String("ownerId = :me"),
			ExpressionAttributeValues: map[string]types.AttributeValue{
				":me": &types.AttributeValueMemberS{Value: ownerID},
			},
		})
		return deleteErr
	}, func(err error) bool {
		// Don't retry ConditionalCheckFailedException (application-level lock contention)
		var ccfe *types.ConditionalCheckFailedException
		if errors.As(err, &ccfe) {
			return false
		}
		return isRetryableError(err)
	})

	var ccfe *types.ConditionalCheckFailedException
	if retryErr != nil && !errors.As(deleteErr, &ccfe) {
		return retryErr
	}

	return nil
}

// key returns the DynamoDB primary key components for the shard lock.
func (d *DynamoLock) key(shardID string) map[string]types.AttributeValue {
	return map[string]types.AttributeValue{
		"pk": &types.AttributeValueMemberS{Value: "SHARD#" + shardID},
		"sk": &types.AttributeValueMemberS{Value: "LOCK"},
	}
}
