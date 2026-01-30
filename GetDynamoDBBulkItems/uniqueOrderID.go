// package main

// import (
// 	"encoding/json"
// 	"flag"
// 	"fmt"
// 	"os"

// 	"github.com/aws/aws-sdk-go/aws"
// 	"github.com/aws/aws-sdk-go/aws/session"
// 	"github.com/aws/aws-sdk-go/service/dynamodb"
// 	"github.com/aws/aws-sdk-go/service/dynamodb/dynamodbattribute"
// )

// const (
// 	tableName = "TradeOrders" // CHANGE THIS
// 	indexName = "SupplierID-OrderID-index"
// )

// type OrderInput struct {
// 	SupplierID string `json:"supplierID"`
// 	OrderID    string `json:"orderID"`
// }

// type OrderItem struct {
// 	Deleted bool `json:"Deleted"`
// }

// func main() {
// 	// Command-line flag for JSON file path
// 	jsonFile := flag.String("file", "unmatched_orders.json", "/Users/mac/Desktop/MyDesktop/Work/Scripts/Scripts/GetDynamoDBBulkItems/unmatched_orders.json")
// 	flag.Parse()

// 	// Read local JSON file
// 	data, err := os.ReadFile(*jsonFile)
// 	if err != nil {
// 		fmt.Printf("Error reading file %s: %v\n", *jsonFile, err)
// 		os.Exit(1)
// 	}

// 	var orders []OrderInput
// 	if err := json.Unmarshal(data, &orders); err != nil {
// 		fmt.Printf("Error parsing JSON: %v\n", err)
// 		os.Exit(1)
// 	}

// 	if len(orders) == 0 {
// 		fmt.Println("No orders found in JSON.")
// 		return
// 	}

// 	// Initialize AWS session
// 	sess, err := session.NewSession(&aws.Config{
// 		Region: aws.String("us-east-1"), // CHANGE TO YOUR REGION
// 	})
// 	if err != nil {
// 		fmt.Printf("Error creating AWS session: %v\n", err)
// 		os.Exit(1)
// 	}
// 	svc := dynamodb.New(sess)

// 	count := 0
// 	for _, order := range orders {
// 		order.SupplierID = "SAGREETINGS"
// 		// Validate input
// 		if order.SupplierID == "" || order.OrderID == "" {
// 			fmt.Printf("Skipping invalid order: %+v\n", order)
// 			continue
// 		}

// 		// Query DynamoDB using GSI
// 		input := &dynamodb.QueryInput{
// 			TableName:              aws.String(tableName),
// 			IndexName:              aws.String(indexName),
// 			KeyConditionExpression: aws.String("#sid = :sid AND #oid = :oid"),
// 			ExpressionAttributeNames: map[string]*string{
// 				"#sid": aws.String("SupplierID"),
// 				"#oid": aws.String("OrderID"),
// 			},
// 			ExpressionAttributeValues: map[string]*dynamodb.AttributeValue{
// 				":sid": {S: aws.String(order.SupplierID)},
// 				":oid": {S: aws.String(order.OrderID)},
// 			},
// 			ProjectionExpression: aws.String("Deleted"),
// 		}

// 		result, err := svc.Query(input)
// 		if err != nil {
// 			fmt.Printf("DDB Query error [SupplierID=%s, OrderID=%s]: %v\n", order.SupplierID, order.OrderID, err)
// 			continue
// 		}

// 		if len(result.Items) == 0 {
// 			// Order not found in DDB
// 			continue
// 		}

// 		var item OrderItem
// 		if err := dynamodbattribute.UnmarshalMap(result.Items[0], &item); err != nil {
// 			fmt.Printf("Unmarshal error [SupplierID=%s, OrderID=%s]: %v\n", order.SupplierID, order.OrderID, err)
// 			continue
// 		}

// 		if item.Deleted {
// 			count++
// 			fmt.Printf("DELETED: SupplierID=%s, OrderID=%s\n", order.SupplierID, order.OrderID)
// 		}
// 	}

// 	fmt.Printf("\nTotal orders with Deleted = true: %d out of %d\n", count, len(orders))
// }
