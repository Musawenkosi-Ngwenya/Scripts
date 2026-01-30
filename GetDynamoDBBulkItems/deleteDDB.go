package main

import (
	"context"
	"fmt"
	"log"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb/types"
)

func main() {
	ctx := context.TODO()

	cfg, err := config.LoadDefaultConfig(
		ctx,
		config.WithSharedConfigProfile("musafc"),
	)
	if err != nil {
		log.Fatalf("failed to load config: %v", err)
	}

	db := dynamodb.NewFromConfig(cfg)

	tableName := "imotoPricelist"
	partitionKey := "26ec4d64-f1e8-44dc-a8a2-3cfc201bd97e|1"
	versionFilter := "20251126100006"

	// Query using correct PK name: PartitionKey
	queryInput := &dynamodb.QueryInput{
		TableName:              aws.String(tableName),
		KeyConditionExpression: aws.String("PartitionKey = :pk"),
		FilterExpression:       aws.String("#v = :ver"),
		ExpressionAttributeNames: map[string]string{
			"#v": "Version",
		},
		ExpressionAttributeValues: map[string]types.AttributeValue{
			":pk":  &types.AttributeValueMemberS{Value: partitionKey},
			":ver": &types.AttributeValueMemberS{Value: versionFilter},
		},
	}

	result, err := db.Query(ctx, queryInput)
	if err != nil {
		log.Fatalf("query failed: %v", err)
	}

	if len(result.Items) == 0 {
		fmt.Println("No items found.")
		return
	}

	fmt.Printf("Found %d items to delete...\n", len(result.Items))

	// Delete using correct key names: PartitionKey + SortKey
	for _, item := range result.Items {
		pk := item["PartitionKey"].(*types.AttributeValueMemberS).Value
		sk := item["SortKey"].(*types.AttributeValueMemberS).Value

		_, err := db.DeleteItem(ctx, &dynamodb.DeleteItemInput{
			TableName: aws.String(tableName),
			Key: map[string]types.AttributeValue{
				"PartitionKey": &types.AttributeValueMemberS{Value: pk},
				"SortKey":      &types.AttributeValueMemberS{Value: sk},
			},
		})
		if err != nil {
			log.Printf("failed to delete PK=%s SK=%s: %v\n", pk, sk, err)
			continue
		}

		fmt.Printf("Deleted: PK=%s SK=%s\n", pk, sk)
	}

	fmt.Println("Delete completed.")
}
