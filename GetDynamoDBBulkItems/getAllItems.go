// package main

// import (
// 	"context"
// 	"encoding/csv"
// 	"encoding/json"
// 	"fmt"
// 	"log"
// 	"os"
// 	"time"

// 	"github.com/xuri/excelize/v2"

// 	"github.com/aws/aws-sdk-go-v2/aws"
// 	"github.com/aws/aws-sdk-go-v2/config"
// 	"github.com/aws/aws-sdk-go-v2/feature/dynamodb/attributevalue"
// 	"github.com/aws/aws-sdk-go-v2/service/dynamodb"
// 	"github.com/aws/aws-sdk-go-v2/service/dynamodb/types"
// )

// // Define your struct
// type OrderInfo struct {
// 	SupplierID           string    `json:"SupplierID,omitempty"`
// 	SortKey              string    `json:"SortKey,omitempty"`
// 	OrderID              string    `json:"OrderID"`
// 	BranchID             string    `json:"BranchID"`
// 	AccountID            string    `json:"AccountID"`
// 	AccountName          string    `json:"AccountName,omitempty"`
// 	UserID               string    `json:"UserID"`
// 	RepID                string    `json:"RepID,omitempty"`
// 	Type                 string    `json:"Type"`
// 	CreateDate           time.Time `json:"CreateDate"`
// 	RequiredByDate       time.Time `json:"RequiredByDate"`
// 	Reference            string    `json:"Reference"`
// 	Comments             string    `json:"Comments"`
// 	Route                string    `json:"Route,omitempty"`
// 	Status               string    `json:"Status,omitempty"`
// 	Longitude            string    `json:"Longitude,omitempty"`
// 	Latitude             string    `json:"Latitude,omitempty"`
// 	TotalExcl            float64   `json:"TotalExcl,omitempty"`
// 	RepChangedPrice      bool      `json:"RepChangedPrice,omitempty"`
// 	ClientOrderID        string    `json:"ClientOrderID,omitempty"`
// 	DeliveryName         string    `json:"DeliveryName"`
// 	DeliveryAddress1     string    `json:"DeliveryAddress1,omitempty"`
// 	DeliveryAddress2     string    `json:"DeliveryAddress2,omitempty"`
// 	DeliveryAddress3     string    `json:"DeliveryAddress3,omitempty"`
// 	DeliveryPostCode     string    `json:"DeliveryPostCode,omitempty"`
// 	DeliveryMethod       string    `json:"DeliveryMethod,omitempty"`
// 	RouteID              string    `json:"RouteID,omitempty"`
// 	ShipmentID           string    `json:"ShipmentID,omitempty"`
// 	PostedToERP          bool      `json:"PostedToERP"`
// 	ERPOrderNumber       string    `json:"ERPOrderNumber"`
// 	ERPStatus            string    `json:"ERPStatus,omitempty"`
// 	Email                string    `json:"Email,omitempty"`
// 	Value                float64   `json:"Value,omitempty"`
// 	Locked               bool      `json:"Locked"`
// 	LockedBy             string    `json:"LockedBy"`
// 	LockedDate           time.Time `json:"LockedDate,omitempty"`
// 	TTL                  string    `json:"TTL"`
// 	PaymentDate          time.Time `json:"PaymentDate,omitempty"`
// 	HaveNotifiedCreator  time.Time `json:"HaveNotifiedCreator,omitempty"`
// 	HaveNotifiedCustomer time.Time `json:"HaveNotifiedCustomer,omitempty"`
// 	WorkflowAllowed      bool      `json:"WorkflowAllowed,omitempty"`
// 	UpdateStockAllowed   bool      `json:"UpdateStockAllowed,omitempty"`
// 	Deleted              bool      `json:"Deleted"`
// 	OrderQuantity        float32   `json:"OrderQuantity"`
// 	Quarter              int       `json:"Quarter"`
// 	OrderCreatedDate     string    `json:"OrderCreatedDate"`
// 	Version              string    `json:"Version"`
// 	Year                 int       `json:"Year"`
// 	Week                 int       `json:"Week"`
// 	CallCycleWeek        int       `json:"CallCycleWeek"`
// }

// func main() {
// 	ctx := context.TODO()
// 	start := time.Now()

// 	cfg, err := config.LoadDefaultConfig(ctx)
// 	if err != nil {
// 		log.Fatalf("failed to load AWS config: %v", err)
// 	}

// 	client := dynamodb.NewFromConfig(cfg)

// 	input := &dynamodb.QueryInput{
// 		TableName:              aws.String("TradeOrders"),
// 		KeyConditionExpression: aws.String("SupplierID = :supplier"),
// 		ExpressionAttributeValues: map[string]types.AttributeValue{
// 			":supplier": &types.AttributeValueMemberS{Value: "VERMONT"},
// 		},
// 	}

// 	var orders []OrderInfo
// 	var lastEvaluatedKey map[string]types.AttributeValue

// 	totalFetched := 0
// 	batchSize := 300

// 	for {
// 		if lastEvaluatedKey != nil {
// 			input.ExclusiveStartKey = lastEvaluatedKey
// 		}

// 		output, err := client.Query(ctx, input)
// 		if err != nil {
// 			log.Fatalf("failed to query items: %v", err)
// 		}

// 		var batch []OrderInfo
// 		err = attributevalue.UnmarshalListOfMaps(output.Items, &batch)
// 		if err != nil {
// 			log.Fatalf("failed to unmarshal items: %v", err)
// 		}

// 		orders = append(orders, batch...)
// 		totalFetched += len(batch)

// 		// 🟢 Progress indicator
// 		fmt.Printf("Fetched %d items so far...\n", totalFetched)

// 		// Process in batches of 50
// 		if len(orders) >= batchSize {
// 			processBatch(orders[:batchSize])
// 			orders = orders[batchSize:]
// 		}

// 		if output.LastEvaluatedKey == nil {
// 			break
// 		}
// 		lastEvaluatedKey = output.LastEvaluatedKey
// 	}

// 	// Process the last remaining batch
// 	if len(orders) > 0 {
// 		processBatch(orders)
// 	}

// 	duration := time.Since(start)
// 	fmt.Printf("✅ Completed fetching in %v seconds\n", duration.Seconds())

// 	// Save all to files
// 	saveToJSON("TradeOrders_VERMONT.json", orders)
// 	saveToCSV("TradeOrders_VERMONT.csv", orders)
// 	saveToExcel("TradeOrders_VERMONT.xlsx", orders)
// 	fmt.Println("💾 Saved results to JSON, CSV, and Excel files.")
// }

// // --- Helper functions ---

// func processBatch(batch []OrderInfo) {
// 	fmt.Printf("Processing batch of %d orders...\n", len(batch))
// 	// Here you could add extra processing per batch if needed
// }

// func saveToJSON(filename string, data []OrderInfo) {
// 	file, err := os.Create(filename)
// 	if err != nil {
// 		log.Fatalf("failed to create JSON file: %v", err)
// 	}
// 	defer file.Close()
// 	encoder := json.NewEncoder(file)
// 	encoder.SetIndent("", "  ")
// 	if err := encoder.Encode(data); err != nil {
// 		log.Fatalf("failed to write JSON: %v", err)
// 	}
// }

// func saveToCSV(filename string, data []OrderInfo) {
// 	file, err := os.Create(filename)
// 	if err != nil {
// 		log.Fatalf("failed to create CSV file: %v", err)
// 	}
// 	defer file.Close()

// 	writer := csv.NewWriter(file)
// 	defer writer.Flush()

// 	// Write header (subset for simplicity)
// 	header := []string{"SupplierID", "OrderID", "AccountName", "BranchID", "TotalExcl", "Status", "CreateDate"}
// 	writer.Write(header)

// 	for _, o := range data {
// 		row := []string{
// 			o.SupplierID,
// 			o.OrderID,
// 			o.AccountName,
// 			o.BranchID,
// 			fmt.Sprintf("%.2f", o.TotalExcl),
// 			o.Status,
// 			o.CreateDate.Format(time.RFC3339),
// 		}
// 		writer.Write(row)
// 	}
// }

// func saveToExcel(filename string, data []OrderInfo) {
// 	f := excelize.NewFile()
// 	sheet := "Orders"
// 	index, err := f.NewSheet(sheet)
// 	if err != nil {
// 		log.Fatalf("failed to create new sheet: %v", err)
// 	}

// 	headers := []string{"SupplierID", "OrderID", "AccountName", "BranchID", "TotalExcl", "Status", "CreateDate"}
// 	for i, h := range headers {
// 		cell, _ := excelize.CoordinatesToCellName(i+1, 1)
// 		f.SetCellValue(sheet, cell, h)
// 	}

// 	for i, o := range data {
// 		row := i + 2
// 		f.SetCellValue(sheet, fmt.Sprintf("A%d", row), o.SupplierID)
// 		f.SetCellValue(sheet, fmt.Sprintf("B%d", row), o.OrderID)
// 		f.SetCellValue(sheet, fmt.Sprintf("C%d", row), o.AccountName)
// 		f.SetCellValue(sheet, fmt.Sprintf("D%d", row), o.BranchID)
// 		f.SetCellValue(sheet, fmt.Sprintf("E%d", row), o.TotalExcl)
// 		f.SetCellValue(sheet, fmt.Sprintf("F%d", row), o.Status)
// 		f.SetCellValue(sheet, fmt.Sprintf("G%d", row), o.CreateDate.Format("2006-01-02 15:04:05"))
// 	}

// 	f.SetActiveSheet(index)
// 	if err := f.SaveAs(filename); err != nil {
// 		log.Fatalf("failed to save Excel file: %v", err)
// 	}
// }
