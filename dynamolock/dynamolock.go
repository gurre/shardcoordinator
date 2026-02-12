// Package dynamolock provides a DynamoDB-based implementation of distributed locking.
package dynamolock

import (
	"context"
	"errors"
	"fmt"
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
	_, err := d.client.PutItem(ctx, &dynamodb.PutItemInput{
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

	var ccfe *types.ConditionalCheckFailedException
	if errors.As(err, &ccfe) {
		return false, nil
	} else if err != nil {
		return false, err
	}

	return true, nil
}

// Renew extends lock TTL if caller is the owner.
// Returns true if renewed, false if ownership changed.
func (d *DynamoLock) Renew(ctx context.Context, shardID string, ownerID string, ttl time.Time) (bool, error) {
	ttlUnix := ttl.Unix()
	_, err := d.client.UpdateItem(ctx, &dynamodb.UpdateItemInput{
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

	var ccfe *types.ConditionalCheckFailedException
	if errors.As(err, &ccfe) {
		return false, nil
	} else if err != nil {
		return false, err
	}

	return true, nil
}

// Release deletes lock if caller is the owner.
// Returns error only for infrastructure failures.
func (d *DynamoLock) Release(ctx context.Context, shardID string, ownerID string) error {
	_, err := d.client.DeleteItem(ctx, &dynamodb.DeleteItemInput{
		TableName:           &d.table,
		Key:                 d.key(shardID),
		ConditionExpression: aws.String("ownerId = :me"),
		ExpressionAttributeValues: map[string]types.AttributeValue{
			":me": &types.AttributeValueMemberS{Value: ownerID},
		},
	})

	var ccfe *types.ConditionalCheckFailedException
	if err != nil && !errors.As(err, &ccfe) {
		return err
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
