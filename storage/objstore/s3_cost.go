package objstore

import (
	"fmt"
)

// S3Usage keeps track of the number of cheap and expensive requests
type S3Usage struct {
	cheapRequests     int
	expensiveRequests int
}

// Cost per 1,000 requests in microdollars (1 dollar = 1,000,000 microdollars)
const (
	cheapCostPerThousand     = 400   // $0.0004 = 400 microdollars
	expensiveCostPerThousand = 5_000 // $0.005 = 5000 microdollars
)

// AddCheapRequest increments the number of cheap requests
func (s *S3Usage) AddCheapRequest() {
	s.cheapRequests++
}

// AddExpensiveRequest increments the number of expensive requests
func (s *S3Usage) AddExpensiveRequest() {
	s.expensiveRequests++
}

// TotalCost calculates the total cost and returns it formatted as USD.
func (s *S3Usage) TotalCost() string {
	// Calculate the total cost in microdollars
	cheapCost := (s.cheapRequests * cheapCostPerThousand) / 1000
	expensiveCost := (s.expensiveRequests * expensiveCostPerThousand) / 1000
	totalMicrodollars := cheapCost + expensiveCost

	// Convert microdollars to dollars and cents
	dollars := totalMicrodollars / 1_000_000
	cents := (totalMicrodollars % 1_000_000) / 10_000
	remainderMicrodollars := (totalMicrodollars % 10_000) / 100

	// Format the cost
	if dollars > 0 || cents > 0 {
		return fmt.Sprintf("$%d.%02d", dollars, cents)
	}
	return fmt.Sprintf("$0.%04d", remainderMicrodollars)
}
