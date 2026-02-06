package main

import (
	"context"
	"fmt"
	"log"
	"time"

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

	tableName := "jackhammerPricelist"
	sellerID := "8743634e-7d8a-4119-a6c3-2071630894bb"

	fmt.Println("=== Starting DynamoDB Bulk Delete ===")
	fmt.Printf("Table: %s\n", tableName)
	fmt.Printf("Seller ID: %s\n", sellerID)
	fmt.Println()

	// Describe the table to get key schema
	fmt.Println("Step 1: Checking table schema...")
	describeOutput, err := db.DescribeTable(ctx, &dynamodb.DescribeTableInput{
		TableName: aws.String(tableName),
	})
	if err != nil {
		log.Fatalf("failed to describe table: %v", err)
	}

	fmt.Println("  Table Key Schema:")
	var pkName, skName string
	for _, key := range describeOutput.Table.KeySchema {
		fmt.Printf("    %s: %s\n", key.KeyType, *key.AttributeName)
		if key.KeyType == types.KeyTypeHash {
			pkName = *key.AttributeName
		} else if key.KeyType == types.KeyTypeRange {
			skName = *key.AttributeName
		}
	}
	fmt.Println()

	if pkName == "" {
		log.Fatalf("Could not determine partition key name from table schema")
	}

	// Scan all items where partition key starts with sellerID
	fmt.Println("Step 2: Scanning items with partition key starting with seller ID...")
	scanStart := time.Now()

	var allItems []map[string]types.AttributeValue
	var lastEvaluatedKey map[string]types.AttributeValue
	scanCount := 0

	for {
		scanCount++
		fmt.Printf("  Scan batch %d...\n", scanCount)

		// Use Scan with filter expression to find all items where PK starts with sellerID
		scanInput := &dynamodb.ScanInput{
			TableName:        aws.String(tableName),
			FilterExpression: aws.String(fmt.Sprintf("begins_with(%s, :sellerID)", pkName)),
			ExpressionAttributeValues: map[string]types.AttributeValue{
				":sellerID": &types.AttributeValueMemberS{Value: sellerID},
			},
			ExclusiveStartKey: lastEvaluatedKey,
		}

		result, err := db.Scan(ctx, scanInput)
		if err != nil {
			log.Fatalf("scan failed: %v", err)
		}

		allItems = append(allItems, result.Items...)
		fmt.Printf("    Retrieved %d items (total so far: %d)\n", len(result.Items), len(allItems))

		if result.LastEvaluatedKey == nil {
			break
		}
		lastEvaluatedKey = result.LastEvaluatedKey
	}

	scanDuration := time.Since(scanStart)
	fmt.Printf("\n✓ Scan completed in %s\n", scanDuration)
	fmt.Printf("  Total items found: %d\n\n", len(allItems))

	if len(allItems) == 0 {
		fmt.Println("No items found. Exiting.")
		return
	}

	// Print first few items to verify
	if len(allItems) > 0 {
		fmt.Println("Sample items found (first 3):")
		limit := 3
		if len(allItems) < limit {
			limit = len(allItems)
		}
		for i := 0; i < limit; i++ {
			pkAttr := allItems[i][pkName]
			if pkMember, ok := pkAttr.(*types.AttributeValueMemberS); ok {
				fmt.Printf("  %d. %s = %s\n", i+1, pkName, pkMember.Value)
			}
		}
		fmt.Println()
	}

	// Batch delete in chunks of 25 (DynamoDB limit)
	fmt.Println("Step 3: Deleting items in batches...")
	deleteStart := time.Now()

	batchSize := 25
	totalDeleted := 0
	skipped := 0
	batchCount := 0
	totalBatches := (len(allItems) + batchSize - 1) / batchSize

	for i := 0; i < len(allItems); i += batchSize {
		batchCount++
		end := i + batchSize
		if end > len(allItems) {
			end = len(allItems)
		}

		batch := allItems[i:end]
		writeRequests := make([]types.WriteRequest, 0, len(batch))

		for _, item := range batch {
			// Build the key based on table schema
			key := make(map[string]types.AttributeValue)

			// Safely extract Partition Key
			pkAttr, pkExists := item[pkName]
			if !pkExists || pkAttr == nil {
				log.Printf("  [WARNING] Skipping item: missing %s", pkName)
				skipped++
				continue
			}
			key[pkName] = pkAttr

			// If there's a sort key, add it
			if skName != "" {
				skAttr, skExists := item[skName]
				if !skExists || skAttr == nil {
					log.Printf("  [WARNING] Skipping item: missing %s", skName)
					skipped++
					continue
				}
				key[skName] = skAttr
			}

			writeRequests = append(writeRequests, types.WriteRequest{
				DeleteRequest: &types.DeleteRequest{
					Key: key,
				},
			})
		}

		if len(writeRequests) == 0 {
			fmt.Printf("  Batch %d/%d: No valid items to delete, skipping\n", batchCount, totalBatches)
			continue
		}

		batchStart := time.Now()
		_, err := db.BatchWriteItem(ctx, &dynamodb.BatchWriteItemInput{
			RequestItems: map[string][]types.WriteRequest{
				tableName: writeRequests,
			},
		})

		if err != nil {
			log.Printf("  [ERROR] Batch %d/%d failed: %v\n", batchCount, totalBatches, err)

			// Print first item in failed batch for debugging
			if len(writeRequests) > 0 {
				fmt.Println("  First item key in failed batch:")
				for k, v := range writeRequests[0].DeleteRequest.Key {
					fmt.Printf("    %s: %v\n", k, v)
				}
			}
			continue
		}

		totalDeleted += len(writeRequests)
		batchDuration := time.Since(batchStart)
		percentage := float64(totalDeleted) / float64(len(allItems)) * 100

		fmt.Printf("  Batch %d/%d: Deleted %d items in %s (Total: %d/%d, %.1f%% complete)\n",
			batchCount, totalBatches, len(writeRequests), batchDuration, totalDeleted, len(allItems), percentage)
	}

	deleteDuration := time.Since(deleteStart)
	totalDuration := time.Since(scanStart)

	fmt.Println("\n=== Delete Operation Summary ===")
	fmt.Printf("✓ Total items deleted: %d\n", totalDeleted)
	if skipped > 0 {
		fmt.Printf("⚠ Total items skipped: %d\n", skipped)
	}
	fmt.Printf("Scan time: %s\n", scanDuration)
	fmt.Printf("Delete time: %s\n", deleteDuration)
	fmt.Printf("Total time: %s\n", totalDuration)
	if totalDeleted > 0 {
		avgTime := deleteDuration.Milliseconds() / int64(totalDeleted)
		fmt.Printf("Average: ~%dms per item\n", avgTime)
	}
	fmt.Println("=== Completed ===")
}
