# Vortex Analytics Go SDK

The Vortex Analytics Go SDK is a lightweight, production-ready library for sending analytics events to the Vortex Analytics platform. It is designed for use in any Go application (backends, CLI tools, services, etc.).

It supports:

- Immediate and batched event tracking
- Automatic retry when the server becomes available
- Local file-based session and device identification
- Verbose logging for debugging
- Thread-safe queue management

## Installation

```bash
go get github.com/vortex-analytics-io/go-sdk
```

## Quick Start

### 1. Initialize the SDK

```go
package main

import (
	"time"
	analytics "github.com/vortex-analytics-io/go-sdk"
)

func main() {
	// Get the singleton instance
	vortex := analytics.Instance()

	// Optional: Enable verbose logging to see network requests
	vortex.SetVerbose(true)

	// Initialize the SDK
	// Parameters: TenantID, ServerURL, Platform, AppVersion, AutoBatching, FlushIntervalSec
	vortex.Init(
		"481b9cfb-4b48-4d99-a310-d383c6cc5d3b",        // Tenant ID
		"https://in.vortexanalytics.io",                // Server URL
		"my_backend_service",                           // Platform
		"1.0.0",                                        // App Version
		true,                                           // Enable auto-batching
		2,                                              // Flush every 2 seconds
	)

	// Wait for background initialization to complete
	time.Sleep(1 * time.Second)
	
	// Your code here...
	
	// Always shutdown when done to flush remaining events
	vortex.Shutdown()
}
```

> ⚠️ Important: `Init()` must be called before sending any events.

### Internal Behavior

On initialization, the system:

1. Loads or generates a persistent device identifier (stored locally)
2. Creates a new session ID
3. Starts a background task to validate the tenant and check server health
4. Enables or disables analytics based on server availability

If the server is unreachable, events are safely queued in memory until connectivity is restored.

## Tracking Events

### Simple Event

```go
vortex.TrackEvent("app_started")
```

### Event with String Payload

```go
vortex.TrackEvent("user_logged_in", "admin_user")
```

### Event with Structured Data

```go
vortex.TrackEvent("order_completed", map[string]any{
	"order_id":     12345,
	"total_amount": 99.99,
	"currency":     "USD",
	"items_count":  3,
})
```

## Custom Data

You can attach custom JSON data to all analytics events sent by the system. This is useful for adding user context, environment details, or other metadata.

### Setting Custom Data

```go
vortex.SetCustomData(map[string]any{
	"user_id":       "user_123",
	"subscription":  "premium",
	"environment":   "production",
	"region":        "us-east-1",
})

// Now all subsequent events include this custom data
vortex.TrackEvent("feature_used", "dark_mode_toggle")
```

### Clearing Custom Data

```go
// Clear all custom data
vortex.SetCustomData(nil)
```

### Behavior

- Custom data is stored persistently and included in every subsequent event
- Persists across multiple `TrackEvent()` calls until explicitly cleared or changed
- Empty custom data is not included in the request payload
- Only valid JSON data is accepted; invalid data is logged as an error

## Batching

### Automatic Batching

When `autoBatching` is enabled during initialization:

```go
vortex.Init(
	tenantId,
	serverUrl,
	platform,
	appVersion,
	true,  // Enable auto-batching
	10,    // Flush every 10 seconds
)
```

- Events tracked via `TrackEvent()` are queued automatically
- The system flushes the queue every `flushIntervalSec` seconds
- If the server is unreachable, events remain queued until the server responds

### Manual Batching

Manual batching allows you to explicitly control when events are sent (e.g., at the end of a transaction or API request).

```go
// Add events to the manual batch
vortex.BatchedTrackEvent("transaction_started", "checkout")
vortex.BatchedTrackEvent("payment_processed", map[string]any{
	"amount":   49.99,
	"method":   "credit_card",
	"status":   "success",
})

// Send all batched events in a single request
vortex.FlushManualBatch()
```

## Lifecycle Handling

Always call `Shutdown()` when your application exits to ensure buffered events are flushed to the network.

### Console Application

```go
package main

import (
	"fmt"
	"time"
	analytics "github.com/vortex-analytics-io/go-sdk"
)

func main() {
	vortex := analytics.Instance()
	vortex.Init("tenant-id", "https://analytics.server.com", "cli_app", "1.0.0", true, 5)

	// Application logic...
	vortex.TrackEvent("app_started")
	time.Sleep(2 * time.Second)
	vortex.TrackEvent("app_completed")

	// Shutdown neatly to flush any pending events
	fmt.Println("Shutting down...")
	vortex.Shutdown()
	fmt.Println("Done!")
}
```

### Web Service (e.g., HTTP Handler)

```go
package main

import (
	"net/http"
	"time"
	analytics "github.com/vortex-analytics-io/go-sdk"
)

func init() {
	vortex := analytics.Instance()
	vortex.Init("tenant-id", "https://analytics.server.com", "web_service", "1.0.0", true, 10)
	
	// Register shutdown on service termination
	go func() {
		// Listen for shutdown signal and call Shutdown()
		// (implementation depends on your framework)
	}()
}

func handleRequest(w http.ResponseWriter, r *http.Request) {
	vortex := analytics.Instance()
	vortex.TrackEvent("api_request", map[string]any{
		"path":   r.URL.Path,
		"method": r.Method,
	})
	w.WriteHeader(http.StatusOK)
}
```

Calling `Shutdown()` performs a blocking wait (max 2 seconds) to attempt a final data flush before the process terminates.

## Example Usage

Here's a complete example demonstrating all features:

```go
package main

import (
	"fmt"
	"time"
	analytics "github.com/vortex-analytics-io/go-sdk"
)

func main() {
	fmt.Println("🚀 Starting Vortex Analytics Test...")

	vortex := analytics.Instance()
	vortex.SetVerbose(true)

	vortex.Init(
		"481b9cfb-4b48-4d99-a310-d383c6cc5d3b",
		"https://in.vortexanalytics.io",
		"backend_test",
		"1.0.0",
		true,
		2,
	)

	time.Sleep(1 * time.Second)

	// Simple event
	fmt.Println("\n--- Sending Simple Event ---")
	vortex.TrackEvent("test_simple_event", "Hello from Go!")

	// Event with properties
	fmt.Println("\n--- Sending Event with Properties ---")
	vortex.TrackEvent("test_properties_event", map[string]any{
		"user_role":  "admin",
		"action":     "checkout",
		"amount":     99.99,
	})

	// Set custom data
	fmt.Println("\n--- Setting Custom Data ---")
	vortex.SetCustomData(map[string]any{
		"environment": "testing",
		"tester_name": "John Doe",
	})
	vortex.TrackEvent("event_with_custom_data", "This event has custom data attached")

	// Wait for auto-flush
	fmt.Println("\n⏳ Waiting for auto-flush...")
	time.Sleep(3 * time.Second)

	// Manual batching
	fmt.Println("\n--- Manual Batching ---")
	vortex.BatchedTrackEvent("batch_event_1", "Item 1")
	vortex.BatchedTrackEvent("batch_event_2", map[string]any{"item": 2})
	vortex.FlushManualBatch()

	time.Sleep(1 * time.Second)

	fmt.Println("\n🛑 Shutting down...")
	vortex.Shutdown()

	fmt.Println("✅ Test complete!")
}
```