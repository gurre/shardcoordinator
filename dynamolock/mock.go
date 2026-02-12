package dynamolock

import (
	"context"
	"strconv"
	"sync"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb/types"
)

// MockDynamoClient implements DynamoClient for testing
type MockDynamoClient struct {
	mu          sync.Mutex
	items       map[string]map[string]types.AttributeValue
	putError    error
	updateError error
	deleteError error
}

// NewMockDynamoClient creates a new mock DynamoDB client for testing
func NewMockDynamoClient() *MockDynamoClient {
	return &MockDynamoClient{
		items: make(map[string]map[string]types.AttributeValue),
	}
}

// ClearLock removes a lock from the mock storage (for testing)
func (m *MockDynamoClient) ClearLock(key string) {
	m.mu.Lock()
	defer m.mu.Unlock()
	delete(m.items, key)
}

func (m *MockDynamoClient) PutItem(ctx context.Context, params *dynamodb.PutItemInput, optFns ...func(*dynamodb.Options)) (*dynamodb.PutItemOutput, error) {
	m.mu.Lock()
	defer m.mu.Unlock()

	if m.putError != nil {
		return nil, m.putError
	}

	pk := params.Item["pk"].(*types.AttributeValueMemberS).Value
	sk := params.Item["sk"].(*types.AttributeValueMemberS).Value
	key := pk + "#" + sk

	// Check condition expression: attribute_not_exists(pk) OR (attribute_exists(pk) AND #t < :now)
	if params.ConditionExpression != nil {
		existingItem, exists := m.items[key]

		// First check: attribute_not_exists(pk)
		if exists {
			// Item exists, check second condition: #t < :now
			if existingTTL, ok := existingItem["ttl"].(*types.AttributeValueMemberN); ok {
				ttlValue, _ := strconv.ParseInt(existingTTL.Value, 10, 64)
				nowValue, _ := strconv.ParseInt(params.ExpressionAttributeValues[":now"].(*types.AttributeValueMemberN).Value, 10, 64)

				// If TTL >= now, condition fails (lock is still valid)
				if ttlValue >= nowValue {
					return nil, &types.ConditionalCheckFailedException{Message: aws.String("ConditionalCheckFailed")}
				}
			}
		}
	}

	// Store the item
	m.items[key] = params.Item
	return &dynamodb.PutItemOutput{}, nil
}

func (m *MockDynamoClient) DeleteItem(ctx context.Context, params *dynamodb.DeleteItemInput, optFns ...func(*dynamodb.Options)) (*dynamodb.DeleteItemOutput, error) {
	m.mu.Lock()
	defer m.mu.Unlock()

	if m.deleteError != nil {
		return nil, m.deleteError
	}

	pk := params.Key["pk"].(*types.AttributeValueMemberS).Value
	sk := params.Key["sk"].(*types.AttributeValueMemberS).Value
	key := pk + "#" + sk

	// Check conditional expression: ownerId = :me
	if params.ConditionExpression != nil && *params.ConditionExpression == "ownerId = :me" {
		existingItem, exists := m.items[key]
		if !exists {
			// Item doesn't exist - delete is a no-op, not an error
			return &dynamodb.DeleteItemOutput{}, nil
		}

		ownerID := existingItem["ownerId"].(*types.AttributeValueMemberS).Value
		expectedOwnerID := params.ExpressionAttributeValues[":me"].(*types.AttributeValueMemberS).Value

		if ownerID != expectedOwnerID {
			// Ownership check failed - conditional check fails
			return nil, &types.ConditionalCheckFailedException{Message: aws.String("ConditionalCheckFailed")}
		}
	}

	delete(m.items, key)
	return &dynamodb.DeleteItemOutput{}, nil
}

func (m *MockDynamoClient) UpdateItem(ctx context.Context, params *dynamodb.UpdateItemInput, optFns ...func(*dynamodb.Options)) (*dynamodb.UpdateItemOutput, error) {
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

	// Check condition expression: ownerId = :me AND #t >= :now
	if params.ConditionExpression != nil && *params.ConditionExpression == "ownerId = :me AND #t >= :now" {
		ownerID := item["ownerId"].(*types.AttributeValueMemberS).Value
		expectedOwnerID := params.ExpressionAttributeValues[":me"].(*types.AttributeValueMemberS).Value

		if ownerID != expectedOwnerID {
			return nil, &types.ConditionalCheckFailedException{Message: aws.String("Owner mismatch")}
		}

		// Check TTL >= now
		ttlValue, _ := strconv.ParseInt(item["ttl"].(*types.AttributeValueMemberN).Value, 10, 64)
		nowValue, _ := strconv.ParseInt(params.ExpressionAttributeValues[":now"].(*types.AttributeValueMemberN).Value, 10, 64)

		if ttlValue < nowValue {
			return nil, &types.ConditionalCheckFailedException{Message: aws.String("TTL expired")}
		}
	}

	// Apply the update
	if params.UpdateExpression != nil && *params.UpdateExpression == "SET #t = :n" {
		newTTL := params.ExpressionAttributeValues[":n"].(*types.AttributeValueMemberN).Value
		item["ttl"] = &types.AttributeValueMemberN{Value: newTTL}
		m.items[key] = item
	}

	return &dynamodb.UpdateItemOutput{}, nil
}
