package dynamolock

// NewTestMockClient creates a new mock DynamoDB client for testing in external packages
func NewTestMockClient() *MockDynamoClient {
	return NewMockDynamoClient()
}
