![](./docs/logo/go.sdk.logo.stacked.svg)

A Data Plane SDK for Go. This SDK provides components for creating Go-based data planes that interface with Dataspace
Protocol Control Planes via the `Data Plane Signaling API`. The SDK includes state management, support for reliable
qualities of service, and error handling.

## Main Operations

### 1. Prepare

- Purpose: Prepares for receiving data
- Function: `Prepare(ctx context.Context, message DataFlowPrepareMessage) (*DataFlowResponseMessage, error)`
- Returns: Response message or error

### 2. Start

- Purpose: Initiates a data flow on the provider side
- Function: `Start(ctx context.Context, message DataFlowStartMessage) (*DataFlowResponseMessage, error)`
- Takes: DataFlowStartMessage as input

### 3. Terminate

- Purpose: Ends a data flow
- Function: `Terminate(ctx context.Context, processID string) error`
- Requires: Process ID

### 4. Suspend

- Purpose: Temporarily halts a data flow
- Function: `Suspend(ctx context.Context, processID string) error`
- Requires: Process ID

## Key Features

### State Management

- Maintains different states for data flows:
    - Preparing
    - Prepared
    - Starting
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

## Usage Example

See the examples.
