package dynamodb

import (
	"context"
	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb/types"
	"time"
)

func createTable(db bucketDB) error {
	input := &dynamodb.CreateTableInput{
		TableName:            aws.String(db.tableName),
		BillingMode:          types.BillingModePayPerRequest,
		AttributeDefinitions: ddbBucketStatePrimaryKey{}.AttributeDefinitions(),
		KeySchema:            ddbBucketStatePrimaryKey{}.KeySchema(),
	}
	if _, err := db.ddb.CreateTable(context.Background(), input); err != nil {
		return err
	}

	waiter := dynamodb.NewTableExistsWaiter(db.ddb)
	return waiter.Wait(context.Background(), &dynamodb.DescribeTableInput{
		TableName: aws.String(db.tableName)}, 5*time.Minute)
}

func deleteTable(db bucketDB) error {
	if _, err := db.ddb.DeleteTable(context.Background(), &dynamodb.DeleteTableInput{
		TableName: aws.String(db.tableName),
	}); err != nil {
		return err
	}

	waiter := dynamodb.NewTableNotExistsWaiter(db.ddb)
	return waiter.Wait(context.Background(), &dynamodb.DescribeTableInput{
		TableName: aws.String(db.tableName)}, 5*time.Minute)
}
