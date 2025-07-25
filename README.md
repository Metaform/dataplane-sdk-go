![](./docs/logo/go.sdk.logo.stacked.svg)

A Data Plane SDK for Go. This SDK provides components for creating Go-based data planes that interface with Dataspace
Protocol Control Planes via the `Data Plane Signaling API`. The SDK includes state management, support for reliable
qualities of service, recovery, and error handling.

## Main Operations

### 1. Prepare

- Purpose: Parepares for receiving data
- Function: `Prepare(ctx context.Context, message DataFlowProvisionMessage) (*DataFlowResponseMessage, error)`
- Returns: Response message or error

### 2. Start

- Purpose: Initiates a data flow on the provider side
- Function: `Start(ctx context.Context, message DataFlowStartMessage) (*DataFlowResponseMessage, error)`
- Takes: DataFlowStartMessage as input

### 3. Terminate

- Purpose: Ends a data flow
- Function: `Terminate(ctx context.Context, processId string) error`
- Requires: Process ID

### 4. Suspend

- Purpose: Temporarily halts a data flow
- Function: `Suspend(ctx context.Context, processId string) error`
- Requires: Process ID

### 5. Recover

- Purpose: Handles recovery of data flows
- Function: `Recover(ctx context.Context) error`
- Processes multiple flows that need recovery

## Key Features

### State Management

- Maintains different states for data flows:
    - Preparing
    - Started
    - Terminated
    - Suspended

### Built-in Capabilities

- Deduplication logic for handling duplicate messages
- Transaction support via TransactionContext
- Comprehensive error handling and propagation
- Extension points through callback functions

## Extension Points

- : Custom prepare logic `OnPrepare`
- : Custom start logic `OnStart`
- : Custom termination logic `OnTerminate`
- : Custom suspension logic `OnSuspend`
- : Custom recovery logic `OnRecover`

## Usage Example

``` go
import (
    "context"
    "fmt"
)

func main() {
    // Create handlers for operations
    provisionHandler := func(ctx context.Context, flow *DataFlow) (*DataFlowResponseMessage, error) {
        // handle prepare logic
    }

    startHandler := func(ctx context.Context, flow *DataFlow) (*DataFlowResponseMessage, error) {
        // handle start logic
    }

    terminateHandler := func(ctx context.Context, flow *DataFlow) error {
        // handle termination logic
    }

    suspendHandler := func(ctx context.Context, flow *DataFlow) error {
        // handle suspend logic
    }

    recoverHandler := func(ctx context.Context, flow *DataFlow) error {
        // handle recovery logic
    }

    // Create a store implementation
    store := // implementation of Data PlaneStore interface

    // Create a transaction context implementation
    trxContext := // implementation of TransactionContext interface

    // Use the builder to create the SDK instance
    sdk, err := NewDataPlaneSDKBuilder().
        Store(store).
        TransactionContext(trxContext).
        OnPrepare(provisionHandler).
        OnStart(startHandler).
        OnTerminate(terminateHandler).
        OnSuspend(suspendHandler).
        OnRecover(recoverHandler).
        Build()

    if err != nil {
        fmt.Printf("Failed to launch SDK: %w\n", err)
        return
    }

    // hook handlers to the Data Plane Signaling API 
}
```
