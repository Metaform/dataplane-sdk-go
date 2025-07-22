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
	SourceDataAddress DataAddress
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
	processId := message.ProcessId
	if processId == "" {
		return nil, errors.New("processId cannot be empty")
	}
	var response *DataFlowResponseMessage
	err := dsdk.execute(ctx, func(context.Context) error {
		flow, err := dsdk.Store.FindById(ctx, processId)
		if err != nil && !errors.Is(err, ErrNotFound) {
			return fmt.Errorf("performing de-duplication for %s: %w", processId, err)
		}

		switch {
		case flow != nil && (flow.State == Preparing || flow.State == Prepared):
			// duplicate message, pass to handler to generate a data address if needed (on consumer)
			response, err = dsdk.onPrepare(ctx, flow, dsdk, &ProcessorOptions{Duplicate: true})
			if err != nil {
				return fmt.Errorf("processing data flow: %w", err)
			}
			return nil
		case flow != nil:
			return fmt.Errorf("data flow %s is not in PREPARING or PREPARED state", flow.ID)
		}
		flow = &DataFlow{ID: processId, Consumer: true, State: Preparing} // TODO fill out
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

	return response, err
}

// Start is called on the provider and starts a data flow based on the given start message and execution context.
// It invokes the onStart callback and persists the created flow. Returns a response or an error if the process fails.
func (dsdk *DataPlaneSDK) Start(ctx context.Context, message DataFlowStartMessage) (*DataFlowResponseMessage, error) {
	processId := message.ProcessId
	if processId == "" {
		return nil, errors.New("processId cannot be empty")
	}
	var response *DataFlowResponseMessage
	err := dsdk.execute(ctx, func(context.Context) error {
		flow, err := dsdk.Store.FindById(ctx, processId)
		if err != nil && !errors.Is(err, ErrNotFound) {
			return fmt.Errorf("performing de-duplication for %s: %w", processId, err)
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

			if err := dsdk.Store.Create(ctx, flow); err != nil {
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
			flow = &DataFlow{ID: processId, Consumer: false, State: Starting}
			response, err = dsdk.onStart(ctx, flow, dsdk, &ProcessorOptions{})
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

func (dsdk *DataPlaneSDK) Terminate(ctx context.Context, processId string) error {
	if processId == "" {
		return errors.New("processId cannot be empty")
	}

	return dsdk.execute(ctx, func(ctx context.Context) error {
		flow, err := dsdk.Store.FindById(ctx, processId)
		if err != nil {
			return fmt.Errorf("terminating data flow %s: %w", processId, err)
		}

		if Terminated == flow.State {
			return nil // duplicate message, skip processing
		}

		err = flow.TransitionToTerminated()
		if err != nil {
			return err
		}

		if err := dsdk.onTerminate(ctx, flow); err != nil {
			return fmt.Errorf("terminating data flow %s: %w", flow.ID, err)
		}
		err = dsdk.Store.Save(ctx, flow)
		if err != nil {
			return fmt.Errorf("terminating data flow %s: %w", flow.ID, err)
		}
		return nil
	})
}

func (dsdk *DataPlaneSDK) Suspend(ctx context.Context, processId string) error {
	if processId == "" {
		return errors.New("processId cannot be empty")
	}

	return dsdk.execute(ctx, func(ctx context.Context) error {
		flow, err := dsdk.Store.FindById(ctx, processId)
		if err != nil {
			return fmt.Errorf("suspending data flow %s: %w", processId, err)
		}

		if Suspended == flow.State {
			return nil // duplicate message, skip processing
		}

		err = flow.TransitionToSuspended()
		if err != nil {
			return err
		}
		if err := dsdk.onSuspend(ctx, flow); err != nil {
			return fmt.Errorf("suspending data flow %s: %w", flow.ID, err)
		}
		err = dsdk.Store.Save(ctx, flow)
		if err != nil {
			return fmt.Errorf("suspending data flow %s: %w", flow.ID, err)
		}
		return nil
	})

}

func (dsdk *DataPlaneSDK) Recover(ctx context.Context) error {
	return dsdk.execute(ctx, func(ctx2 context.Context) error {
		iter := dsdk.Store.AcquireDataFlowsForRecovery(ctx)
		if iter == nil {
			return errors.New("failed to create iterator")
		}
		//nolint:errcheck
		defer iter.Close()

		var errs []error
		for iter.Next() {
			flow := iter.Get()
			if flow == nil {
				continue // skip nil flows
			}
			if err := dsdk.onRecover(ctx, flow); err != nil {
				errs = append(errs, fmt.Errorf("data flow %s: %w", flow.ID, err))
			}
		}

		if err := iter.Error(); err != nil {
			return fmt.Errorf("recovering data flows: %w", err)
		}

		return errors.Join(errs...)
	})
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
		return nil, errors.New("onPrepare handler is required")
	}
	if b.sdk.onStart == nil {
		return nil, errors.New("onStart handler is required")
	}
	if b.sdk.onTerminate == nil {
		return nil, errors.New("onTerminate handler is required")
	}
	if b.sdk.onSuspend == nil {
		return nil, errors.New("onSuspend handler is required")
	}
	if b.sdk.onRecover == nil {
		return nil, errors.New("onRecover handler is required")
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
