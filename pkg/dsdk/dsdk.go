package dsdk

import (
	"context"
	"errors"
	"fmt"
	"log"
)

// DataFlowProcessor is an extension point for handling SDK data flow events. Implementations may modify the data flow instance
// which will be persisted by the SDK. If the message is a duplicate, implementations must support idempotent behavior.
type DataFlowProcessor func(context context.Context, flow *DataFlow, sdk *DataPlaneSDK, options *ProcessorOptions) (*DataFlowResponseMessage, error)

type ProcessorOptions struct {
	Duplicate         bool
	SourceDataAddress *DataAddress
}

type DataFlowHandler func(context.Context, *DataFlow) error

type LogMonitor interface {
	Println(v ...any)
	Printf(format string, v ...any)
}

type DataPlaneSDK struct {
	Store      DataplaneStore
	TrxContext TransactionContext
	Monitor    LogMonitor

	onPrepare   DataFlowProcessor
	onStart     DataFlowProcessor
	onTerminate DataFlowHandler
	onSuspend   DataFlowHandler
	onRecover   DataFlowHandler
}

// Prepare is called on the consumer to prepare for receiving data.
// It invokes the onPrepare callback and persists the created flow. Returns a response or an error if the process fails.
func (dsdk *DataPlaneSDK) Prepare(ctx context.Context, message DataFlowPrepareMessage) (*DataFlowResponseMessage, error) {
	processID := message.ProcessID
	if processID == "" {
		return nil, errors.New("processID cannot be empty")
	}
	var response *DataFlowResponseMessage
	err := dsdk.execute(ctx, func(context.Context) error {
		flow, err := dsdk.Store.FindById(ctx, processID)
		if err != nil && !errors.Is(err, ErrNotFound) {
			return fmt.Errorf("performing de-duplication for %s: %w", processID, err)
		}

		switch {
		case flow != nil && (flow.State == Preparing || flow.State == Prepared):
			// duplicate message, pass to handler to generate a data address if needed (on consumer)
			response, err = dsdk.onPrepare(ctx, flow, dsdk, &ProcessorOptions{Duplicate: true})
			if err != nil {
				return fmt.Errorf("processing data flow: %w", err)
			}
			// todo: not sure about this, added because Prepare() has it too
			if err := dsdk.Store.Save(ctx, flow); err != nil {
				return fmt.Errorf("creating data flow: %w", err)
			}
			return nil
		case flow != nil:
			return fmt.Errorf("%w: data flow %s is not in PREPARING or PREPARED state but in %s", ErrConflict, flow.ID, flow.State.String())
			//return NewConflictError(fmt.Sprintf("data flow %s is not in PREPARING or PREPARED state", flow.ID))
		}
		flow, err = NewDataFlowBuilder().ID(processID).
			Consumer(true).
			State(Preparing).
			AgreementID(message.AgreementID).
			DatasetID(message.DatasetID).
			ParticipantID(message.ParticipantID).
			CounterpartyID(message.CounterPartyID).
			DataspaceContext(message.DataspaceContext).
			TransferType(message.TransferType).
			CallbackAddress(message.CallbackAddress).
			Build()

		if err != nil {
			return fmt.Errorf("creating data flow: %w", err)
		}

		response, err = dsdk.onPrepare(ctx, flow, dsdk, &ProcessorOptions{})
		if err != nil {
			return fmt.Errorf("processing data flow %s: %w", flow.ID, err)
		}
		if response.State == Prepared {
			err := flow.TransitionToPrepared()
			if err != nil {
				return err
			}
		} else if response.State == Preparing {
			err := flow.TransitionToPreparing()
			if err != nil {
				return err
			}
		} else {
			return fmt.Errorf("onPrepare returned an invalid state %s", response.State)
		}
		if err := dsdk.Store.Create(ctx, flow); err != nil {
			return fmt.Errorf("creating data flow %s: %w", flow.ID, err)
		}
		return nil
	})

	// fixme: shouldn't we always return a clean nil/error or response/nil tuple?
	return response, err
}

// Start is called on the provider and starts a data flow based on the given start message and execution context.
// It invokes the onStart callback and persists the created flow. Returns a response or an error if the process fails.
func (dsdk *DataPlaneSDK) Start(ctx context.Context, message DataFlowStartMessage) (*DataFlowResponseMessage, error) {
	processID := message.ProcessID
	if processID == "" {
		return nil, errors.New("processID cannot be empty")
	}
	var response *DataFlowResponseMessage
	err := dsdk.execute(ctx, func(context.Context) error {
		flow, err := dsdk.Store.FindById(ctx, processID)
		if err != nil && !errors.Is(err, ErrNotFound) {
			return fmt.Errorf("performing de-duplication for %s: %w", processID, err)
		}

		switch {
		case flow != nil && (flow.State == Starting || flow.State == Started):
			// duplicate message, pass to handler to generate a data address if needed
			response, err = dsdk.onStart(ctx, flow, dsdk, &ProcessorOptions{Duplicate: true, SourceDataAddress: message.SourceDataAddress})
			if err != nil {
				return fmt.Errorf("processing data flow: %w", err)
			}

			err = dsdk.startState(response, flow)
			if err != nil {
				return fmt.Errorf("onStart returned an invalid state: %w", err)
			}

			if err := dsdk.Store.Save(ctx, flow); err != nil {
				return fmt.Errorf("creating data flow: %w", err)
			}
			return nil
		case flow != nil && flow.Consumer && flow.State == Prepared:
			// consumer side, process
			response, err = dsdk.onStart(ctx, flow, dsdk, &ProcessorOptions{SourceDataAddress: message.SourceDataAddress})
			if err != nil {
				return fmt.Errorf("processing data flow: %w", err)
			}

			err = dsdk.startState(response, flow)
			if err != nil {
				return fmt.Errorf("onStart returned an invalid state: %w", err)
			}

			if err := dsdk.Store.Save(ctx, flow); err != nil {
				return fmt.Errorf("updating data flow: %w", err)
			}

			return nil
		case flow == nil:
			// provider side, process
			flow, err = NewDataFlowBuilder().ID(processID).
				State(Starting).
				AgreementID(message.AgreementID).
				DatasetID(message.DatasetID).
				ParticipantID(message.ParticipantID).
				CounterpartyID(message.CounterPartyID).
				DataspaceContext(message.DataspaceContext).
				TransferType(message.TransferType).
				CallbackAddress(message.CallbackAddress).
				Build()
			if err != nil {
				return fmt.Errorf("creating data flow: %w", err)
			}
			response, err = dsdk.onStart(ctx, flow, dsdk, &ProcessorOptions{SourceDataAddress: message.SourceDataAddress})
			if err != nil {
				return fmt.Errorf("processing data flow: %w", err)
			}

			err = dsdk.startState(response, flow)
			if err != nil {
				return fmt.Errorf("onStart returned an invalid state: %w", err)
			}

			if err := dsdk.Store.Create(ctx, flow); err != nil {
				return fmt.Errorf("creating data flow: %w", err)
			}
			return nil
		default:
			return fmt.Errorf("data flow %s is not in STARTED state: %s", flow.ID, flow.State)
		}
	})

	return response, err

}

func (dsdk *DataPlaneSDK) Terminate(ctx context.Context, processID string) error {
	if processID == "" {
		return errors.New("processID cannot be empty")
	}

	return dsdk.execute(ctx, func(ctx context.Context) error {
		flow, err := dsdk.Store.FindById(ctx, processID)
		if err != nil {
			return fmt.Errorf("terminating data flow %s: %w", processID, err)
		}

		if Terminated == flow.State {
			return nil // duplicate message, skip processing
		}

		if err := dsdk.onTerminate(ctx, flow); err != nil {
			return fmt.Errorf("terminating data flow %s: %w", flow.ID, err)
		}

		err = flow.TransitionToTerminated()
		if err != nil {
			return err
		}

		err = dsdk.Store.Save(ctx, flow)
		if err != nil {
			return fmt.Errorf("terminating data flow %s: %w", flow.ID, err)
		}
		return nil
	})
}

func (dsdk *DataPlaneSDK) Suspend(ctx context.Context, processID string) error {
	if processID == "" {
		return errors.New("processID cannot be empty")
	}

	return dsdk.execute(ctx, func(ctx context.Context) error {
		flow, err := dsdk.Store.FindById(ctx, processID)
		if err != nil {
			return fmt.Errorf("suspending data flow %s: %w", processID, err)
		}

		if Suspended == flow.State {
			return nil // duplicate message, skip processing
		}

		if err := dsdk.onSuspend(ctx, flow); err != nil {
			return fmt.Errorf("suspending data flow %s: %w", flow.ID, err)
		}
		err = flow.TransitionToSuspended()
		if err != nil {
			return err
		}

		err = dsdk.Store.Save(ctx, flow)
		if err != nil {
			return fmt.Errorf("suspending data flow %s: %w", flow.ID, err)
		}
		return nil
	})

}

func (dsdk *DataPlaneSDK) Status(ctx context.Context, id string) (*DataFlow, error) {
	var flow *DataFlow
	err := dsdk.execute(ctx, func(ctx context.Context) error {
		found, err := dsdk.Store.FindById(ctx, id)
		if err != nil {
			return err
		}
		flow = found
		return nil
	})
	return flow, err
}

func (dsdk *DataPlaneSDK) startState(response *DataFlowResponseMessage, flow *DataFlow) error {
	if response.State == Started {
		err := flow.TransitionToStarted()
		if err != nil {
			return err
		}
	} else if response.State == Starting {
		err := flow.TransitionToStarting()
		if err != nil {
			return err
		}
	} else {
		return fmt.Errorf("onStart returned an invalid state %s", response.State)
	}
	return nil
}

func (dsdk *DataPlaneSDK) execute(ctx context.Context, callback func(ctx2 context.Context) error) error {
	select {
	case <-ctx.Done():
		return ctx.Err()
	default:
		return dsdk.TrxContext.Execute(ctx, callback)
	}
}

type DataPlaneSDKBuilder struct {
	sdk *DataPlaneSDK
}

func NewDataPlaneSDKBuilder() *DataPlaneSDKBuilder {
	return &DataPlaneSDKBuilder{
		sdk: &DataPlaneSDK{},
	}
}

func (b *DataPlaneSDKBuilder) Store(store DataplaneStore) *DataPlaneSDKBuilder {
	b.sdk.Store = store
	return b
}

func (b *DataPlaneSDKBuilder) TransactionContext(trxContext TransactionContext) *DataPlaneSDKBuilder {
	b.sdk.TrxContext = trxContext
	return b
}

func (b *DataPlaneSDKBuilder) OnPrepare(processor DataFlowProcessor) *DataPlaneSDKBuilder {
	b.sdk.onPrepare = processor
	return b
}

func (b *DataPlaneSDKBuilder) OnStart(processor DataFlowProcessor) *DataPlaneSDKBuilder {
	b.sdk.onStart = processor
	return b
}

func (b *DataPlaneSDKBuilder) OnTerminate(handler DataFlowHandler) *DataPlaneSDKBuilder {
	b.sdk.onTerminate = handler
	return b
}

func (b *DataPlaneSDKBuilder) OnSuspend(handler DataFlowHandler) *DataPlaneSDKBuilder {
	b.sdk.onSuspend = handler
	return b
}

func (b *DataPlaneSDKBuilder) OnRecover(handler DataFlowHandler) *DataPlaneSDKBuilder {
	b.sdk.onRecover = handler
	return b
}

func (b *DataPlaneSDKBuilder) Build() (*DataPlaneSDK, error) {
	if b.sdk.Store == nil {
		return nil, errors.New("store is required")
	}
	if b.sdk.TrxContext == nil {
		return nil, errors.New("transaction context is required")
	}
	if b.sdk.onPrepare == nil {
		b.sdk.onPrepare = func(context context.Context, flow *DataFlow, sdk *DataPlaneSDK, options *ProcessorOptions) (*DataFlowResponseMessage, error) {
			return &DataFlowResponseMessage{
				DataplaneID: "TODO_REPLACE_ME",
				DataAddress: &flow.DestinationDataAddress,
				State:       Prepared,
				Error:       ""}, nil
		}
	}
	if b.sdk.onStart == nil {
		b.sdk.onStart = func(context context.Context, flow *DataFlow, sdk *DataPlaneSDK, options *ProcessorOptions) (*DataFlowResponseMessage, error) {
			return &DataFlowResponseMessage{
				State:       Started,
				DataplaneID: "TODO_REPLACE_ME",
				DataAddress: &flow.DestinationDataAddress,
				Error:       ""}, nil
		}
	}
	if b.sdk.onTerminate == nil {
		b.sdk.onTerminate = func(context context.Context, flow *DataFlow) error {
			return nil
		}
	}
	if b.sdk.onSuspend == nil {
		b.sdk.onSuspend = func(context context.Context, flow *DataFlow) error {
			return nil
		}
	}
	if b.sdk.onRecover == nil {
		b.sdk.onRecover = func(context context.Context, flow *DataFlow) error {
			return nil
		}
	}
	if b.sdk.Monitor == nil {
		b.sdk.Monitor = defaultLogMonitor{}
	}
	return b.sdk, nil
}

type defaultLogMonitor struct {
}

func (d defaultLogMonitor) Println(v ...any) {
	log.Println(v...)
}

func (d defaultLogMonitor) Printf(format string, v ...any) {
	log.Printf(format, v...)
}
