package dynamodb

import (
	"context"
	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/credentials"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb/types"
	"os"
	"testing"
	"time"

	"github.com/splashing-atom/leakybucket/test"

	"github.com/stretchr/testify/require"
)

func testRequiredEnv(t *testing.T, key string) string {
	val, ok := os.LookupEnv(key)
	if !ok {
		t.Logf("required test env var %s not set", key)
		t.FailNow()
	}

	return val
}

func testConfig(t *testing.T) *aws.Config {
	customResolver := aws.EndpointResolverWithOptionsFunc(
		func(service, region string, opts ...interface{}) (aws.Endpoint, error) {
			return aws.Endpoint{
				PartitionID:   "aws",
				URL:           testRequiredEnv(t, "AWS_DYNAMO_ENDPOINT"),
				SigningRegion: region,
			}, nil
		})

	awsCfg, err := config.LoadDefaultConfig(context.Background(),
		config.WithRegion("us-east-1"),
		config.WithEndpointResolverWithOptions(customResolver),
		config.WithCredentialsProvider(credentials.NewStaticCredentialsProvider("foo", "foo", "foo")),
	)
	require.NoError(t, err)
	return &awsCfg
}

func testStorage(t *testing.T) *Storage {
	table := "test-table"

	awsCfg := testConfig(t)

	ddb := dynamodb.NewFromConfig(*awsCfg)
	db := bucketDB{
		ddb:       ddb,
		tableName: table,
	}
	// ensure we're working with a clean table
	deleteTable(db)
	err := createTable(db)
	require.NoError(t, err)
	storage, err := New(context.Background(), table, ddb, 10*time.Second)
	require.NoError(t, err)

	return storage
}

func TestCreate(t *testing.T) {
	test.CreateTest(testStorage(t))(t)
}

func TestAdd(t *testing.T) {
	test.AddTest(testStorage(t))(t)
}

func TestThreadSafeAdd(t *testing.T) {
	test.ThreadSafeAddTest(testStorage(t))(t)
}

func TestReset(t *testing.T) {
	test.AddResetTest(testStorage(t))(t)
}

func TestFindOrCreate(t *testing.T) {
	test.FindOrCreateTest(testStorage(t))(t)
}

func TestBucketInstanceConsistencyTest(t *testing.T) {
	test.BucketInstanceConsistencyTest(testStorage(t))(t)
}

// package specific tests
func TestNoTable(t *testing.T) {
	awsCfg := testConfig(t)
	ddb := dynamodb.NewFromConfig(*awsCfg)
	_, err := New(context.Background(), "doesntmatter", ddb, 10*time.Second)
	require.Error(t, err)
}

// TestBucketTTL makes sure the TTL field is being set correctly for dynamodb. The tricky part is
// the actual deletion of the bucket is non-deterministic so there are two success modes:
// - the bucket has been deleted -> we should get an `errBucketNotFound`
// - the bucket has not been deleted -> the TTL field should be set to a time before now
func TestBucketTTL(t *testing.T) {
	s := testStorage(t)
	s.db.ttl = time.Second

	_, err := s.db.ddb.UpdateTimeToLive(context.Background(), &dynamodb.UpdateTimeToLiveInput{
		TableName: aws.String(s.db.tableName),
		TimeToLiveSpecification: &types.TimeToLiveSpecification{
			AttributeName: aws.String("_ttl"),
			Enabled:       aws.Bool(true),
		},
	})
	require.NoError(t, err)
	time.Sleep(time.Second)

	bucket, err := s.Create("testbucket", 5, time.Second)
	require.NoError(t, err)

	time.Sleep(s.db.ttl + 10*time.Second)
	dbBucket, err := s.db.bucket(context.Background(), "testbucket")
	if err == nil {
		t.Log("bucket not yet deleted. TTL: ", dbBucket.TTL)
		require.NotNil(t, dbBucket)
		require.True(t, dbBucket.TTL.Before(time.Now()))
	} else {
		t.Log("bucket deleted")
		require.Equal(t, errBucketNotFound, err)
	}

	_, err = bucket.Add(1)
	require.NoError(t, err)
}
