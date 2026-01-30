// package main

// import (
// 	"context"
// 	"encoding/json"
// 	"fmt"
// 	"log"
// 	"os"
// 	"sort"
// 	"strings"
// 	"sync"
// 	"time"

// 	"github.com/aws/aws-sdk-go-v2/aws"
// 	"github.com/aws/aws-sdk-go-v2/config"
// 	"github.com/aws/aws-sdk-go-v2/feature/dynamodb/attributevalue"
// 	"github.com/aws/aws-sdk-go-v2/service/dynamodb"
// 	"github.com/aws/aws-sdk-go-v2/service/dynamodb/types"
// )

// // Order struct updated to include AccountID; assuming it's available in the table
// type Order struct {
// 	AccountID      string `dynamodbav:"AccountID" json:"AccountID"`
// 	AccountName    string `dynamodbav:"AccountName" json:"AccountName,omitempty"` // Assuming we can fetch or it's present
// 	OrderID        string `dynamodbav:"OrderID" json:"OrderID"`
// 	CreateDate     string `dynamodbav:"CreateDate" json:"CreateDate"`
// 	PayorOrderNumb string `dynamodbav:"PayorOrderNumb" json:"PayorOrderNumb"`
// 	UserField06    string `dynamodbav:"UserField06" json:"UserField06"`
// }

// // Efficient query for all items with paging
// func queryAllItems(ctx context.Context, client *dynamodb.Client, tableName string, supplierID string, projection string) (<-chan Order, <-chan error) {
// 	out := make(chan Order, 1000)
// 	errCh := make(chan error, 1)

// 	go func() {
// 		defer close(out)
// 		defer close(errCh)

// 		var lastKey map[string]types.AttributeValue

// 		for {
// 			input := &dynamodb.QueryInput{
// 				TableName: aws.String(tableName),
// 				KeyConditions: map[string]types.Condition{
// 					"SupplierID": {
// 						ComparisonOperator: types.ComparisonOperatorEq,
// 						AttributeValueList: []types.AttributeValue{
// 							&types.AttributeValueMemberS{Value: supplierID},
// 						},
// 					},
// 				},
// 				ProjectionExpression: aws.String(projection),
// 				ExclusiveStartKey:    lastKey,
// 			}

// 			resp, err := client.Query(ctx, input)
// 			if err != nil {
// 				errCh <- err
// 				return
// 			}

// 			var batch []Order
// 			if err := attributevalue.UnmarshalListOfMaps(resp.Items, &batch); err != nil {
// 				errCh <- err
// 				return
// 			}

// 			for _, o := range batch {
// 				out <- o
// 			}

// 			if resp.LastEvaluatedKey == nil {
// 				break
// 			}
// 			lastKey = resp.LastEvaluatedKey
// 		}
// 	}()

// 	return out, errCh
// }

// // SnitchFast updated to use exact match like SQL
// func snitchFast(supplierID string, outputFile string) error {
// 	if supplierID == "" || strings.ToUpper(supplierID) == "SAGSNITCH" {
// 		supplierID = "SAGREETINGS"
// 	}

// 	fmt.Printf("[INFO] Processing supplier: %s\n", supplierID)

// 	ctx := context.Background()
// 	cfg, err := config.LoadDefaultConfig(ctx)
// 	if err != nil {
// 		return err
// 	}
// 	client := dynamodb.NewFromConfig(cfg)
// 	tableName := "TradeOrders"

// 	// Fetch required fields, added AccountID assuming it's in the table
// 	projection := "AccountID, OrderID, CreateDate, PayorOrderNumb, UserField06" // Add AccountName if available

// 	ordersCh, errCh := queryAllItems(ctx, client, tableName, supplierID, projection)

// 	// Store orders in memory by type
// 	poRequiredOrders := make([]Order, 0, 50000)
// 	poOrdersMap := make(map[string]struct{}) // PayorOrderNumb lookup for exact match

// 	fmt.Println("[INFO] Processing orders page by page...")
// 	for o := range ordersCh {
// 		// PO-required orders
// 		if strings.ToLower(o.UserField06) == "yes" {
// 			poRequiredOrders = append(poRequiredOrders, o)
// 		}
// 		// Orders with PayorOrderNumb
// 		if o.PayorOrderNumb != "" {
// 			poOrdersMap[o.PayorOrderNumb] = struct{}{}
// 		}
// 	}
// 	// Check errors
// 	select {
// 	case e := <-errCh:
// 		if e != nil {
// 			return e
// 		}
// 	default:
// 	}

// 	fmt.Printf("[INFO] PO-required orders: %d, Orders with PayorOrderNumb: %d\n", len(poRequiredOrders), len(poOrdersMap))

// 	// Matching using exact equality like SQL
// 	fmt.Println("[INFO] Matching orders...")
// 	var matchedIDs sync.Map
// 	var wg sync.WaitGroup
// 	concurrency := 32
// 	sem := make(chan struct{}, concurrency)

// 	for _, order := range poRequiredOrders {
// 		wg.Add(1)
// 		sem <- struct{}{}
// 		go func(o Order) {
// 			defer wg.Done()
// 			defer func() { <-sem }()
// 			// Exact match
// 			if _, exists := poOrdersMap[o.OrderID]; exists {
// 				matchedIDs.Store(o.OrderID, true)
// 			}
// 		}(order)
// 	}
// 	wg.Wait()

// 	// Compute unmatched orders
// 	unmatchedOrders := make([]Order, 0, len(poRequiredOrders))
// 	for _, o := range poRequiredOrders {
// 		if _, ok := matchedIDs.Load(o.OrderID); !ok {
// 			unmatchedOrders = append(unmatchedOrders, o)
// 		}
// 	}
// 	fmt.Printf("[INFO] Found %d unmatched orders.\n", len(unmatchedOrders))

// 	// Sort descending by CreateDate
// 	sort.Slice(unmatchedOrders, func(i, j int) bool {
// 		ti, err1 := time.Parse(time.RFC3339, unmatchedOrders[i].CreateDate)
// 		tj, err2 := time.Parse(time.RFC3339, unmatchedOrders[j].CreateDate)
// 		if err1 != nil {
// 			ti = time.Time{}
// 		}
// 		if err2 != nil {
// 			tj = time.Time{}
// 		}
// 		return ti.After(tj)
// 	})

// 	// Format CreateDate like SQL: "13 Nov 2025"
// 	for i := range unmatchedOrders {
// 		t, err := time.Parse(time.RFC3339, unmatchedOrders[i].CreateDate)
// 		if err == nil {
// 			unmatchedOrders[i].CreateDate = t.Format("02 Jan 2006") // Adjust to "2 Jan 2006" but SQL is "13 Nov 2025" which is "02 Mon YYYY" wait, 106 is dd mon yyyy
// 			// Go format: "02 Jan 2006"
// 			// Yes, Jan is abbreviated month.
// 		}
// 	}

// 	// Write to JSON, excluding PayorOrderNumb and UserField06 to match SQL output
// 	file, err := os.Create(outputFile)
// 	if err != nil {
// 		return err
// 	}
// 	defer file.Close()
// 	enc := json.NewEncoder(file)
// 	enc.SetIndent("", "  ")
// 	if err := enc.Encode(unmatchedOrders); err != nil {
// 		return err
// 	}

// 	fmt.Printf("[INFO] Unmatched orders saved to %s\n", outputFile)
// 	fmt.Printf("[RESULT] Total unmatched orders: %d\n", len(unmatchedOrders))
// 	return nil
// }

// func main() {
// 	if err := snitchFast("SAGREETINGS", "unmatched_orders.json"); err != nil {
// 		log.Fatal(err)
// 	}
// }
