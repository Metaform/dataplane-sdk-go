//  Copyright (c) 2025 Metaform Systems, Inc
//
//  This program and the accompanying materials are made available under the
//  terms of the Apache License, Version 2.0 which is available at
//  https://www.apache.org/licenses/LICENSE-2.0
//
//  SPDX-License-Identifier: Apache-2.0
//
//  Contributors:
//       Metaform Systems, Inc. - initial API and implementation
//

package provider

import (
	"context"
	"errors"
	"fmt"
	"github.com/metaform/dataplane-sdk-go/examples/common"
	"github.com/metaform/dataplane-sdk-go/examples/natsservices"
	"github.com/metaform/dataplane-sdk-go/pkg/dsdk"
	"github.com/metaform/dataplane-sdk-go/pkg/memory"
	"log"
	"net/http"
)

// ProviderDataPlane demonstrates how to use the Data Plane SDK. This implementation supports pull event streaming.
type ProviderDataPlane struct {
	api                   *dsdk.DataPlaneApi
	signalingServer       *http.Server
	authService           *natsservices.AuthService
	connectionInvalidator ConnectionInvalidator
	publisherService      *EventPublisherService
}

func NewDataPlane(authService *natsservices.AuthService,
	invalidator ConnectionInvalidator,
	publisherService *EventPublisherService) (*ProviderDataPlane, error) {
	providerDataPlane := &ProviderDataPlane{authService: authService, connectionInvalidator: invalidator, publisherService: publisherService}

	builder := dsdk.NewDataPlaneSDKBuilder()
	store := memory.NewInMemoryStore()
	sdk, err := builder.Store(store).
		TransactionContext(memory.InMemoryTrxContext{}).
		OnPrepare(providerDataPlane.prepareProcessor).
		OnStart(providerDataPlane.startProcessor).
		OnRecover(providerDataPlane.noopHandler).
		OnSuspend(providerDataPlane.suspendProcessor).
		OnTerminate(providerDataPlane.terminateProcessor).
		OnRecover(providerDataPlane.noopHandler).
		Build()
	if err != nil {
		return nil, err
	}

	providerDataPlane.api = dsdk.NewDataPlaneApi(sdk)

	return providerDataPlane, nil
}

func (d *ProviderDataPlane) Init() {
	d.signalingServer = common.NewSignalingServer(d.api, common.ProviderSignalingPort)

	// Start signaling server
	go func() {
		log.Printf("[Provider Data Plane] Signaling server listening on port %d\n", common.ProviderSignalingPort)
		if err := d.signalingServer.ListenAndServe(); err != nil && !errors.Is(err, http.ErrServerClosed) {
			log.Fatalf("Provider signaling server failed to start: %v", err)
		}
	}()
}

func (d *ProviderDataPlane) Shutdown(ctx context.Context) {
	if d.signalingServer != nil {
		if err := d.signalingServer.Shutdown(ctx); err != nil {
			log.Printf("Provider signaling server shutdown error: %v", err)
		}
	}
	log.Println("Provider data plane shutdown")
}

func (d *ProviderDataPlane) prepareProcessor(_ context.Context,
	_ *dsdk.DataFlow,
	_ *dsdk.DataPlaneSDK,
	_ *dsdk.ProcessorOptions) (*dsdk.DataFlowResponseMessage, error) {
	return nil, errors.New("not supported on provider")
}

func (d *ProviderDataPlane) startProcessor(_ context.Context,
	flow *dsdk.DataFlow,
	_ *dsdk.DataPlaneSDK,
	options *dsdk.ProcessorOptions) (*dsdk.DataFlowResponseMessage, error) {
	if options.Duplicate {
		// Perform de-duplication. This code path is not needed, but it demonstrates how de-deduplication can be handled
	}

	err := flow.TransitionToStarting()
	if err != nil {
		return nil, err
	}
	token, err := d.authService.CreateToken(flow.ID, true)
	if err != nil {
		return nil, err
	}

	channel := flow.ID + "." + natsservices.ForwardSuffix
	replyChannel := flow.ID + "." + natsservices.ReplySuffix
	da, err := dsdk.NewDataAddressBuilder().
		Property(dsdk.EndpointType, natsservices.NATSEndpointType).
		Property(dsdk.EndpointKey, natsservices.NatsUrl).
		EndpointProperty(natsservices.TokenKey, "string", token).
		EndpointProperty(natsservices.ChannelKey, "string", channel).
		EndpointProperty(natsservices.ReplyChannelKey, "string", replyChannel).
		Build()
	if err != nil {
		return nil, fmt.Errorf("failed to build data address: %w", err)
	}
	err = flow.TransitionToStarted()
	if err != nil {
		return nil, err
	}

	// Start publishing events. In a real system, this could be done via a queue or notification mechanism
	d.publisherService.Start(channel)

	log.Printf("[Provider Data Plane] Starting transfer for %s\n", flow.CounterPartyID)
	return &dsdk.DataFlowResponseMessage{State: flow.State, DataAddress: da}, nil
}

func (d *ProviderDataPlane) suspendProcessor(_ context.Context, flow *dsdk.DataFlow) error {
	err := flow.TransitionToSuspended()
	channel := flow.ID + "." + natsservices.ForwardSuffix
	d.publisherService.Terminate(channel)
	if err != nil {
		return err
	}
	log.Printf("[Provider Data Plane] Suspending transfer for %s\n", flow.CounterPartyID)
	return d.invalidate(flow)
}

func (d *ProviderDataPlane) terminateProcessor(_ context.Context, flow *dsdk.DataFlow) error {
	err := flow.TransitionToTerminated()
	channel := flow.ID + "." + natsservices.ForwardSuffix
	d.publisherService.Terminate(channel)
	if err != nil {
		return err
	}
	log.Printf("[Provider Data Plane] Terminating transfer for %s\n", flow.CounterPartyID)
	return d.invalidate(flow)
}

func (d *ProviderDataPlane) invalidate(flow *dsdk.DataFlow) error {
	// Connection terminated after to avoid case where a client reconnects before the token is invalidated
	defer d.connectionInvalidator.InvalidateConnection(flow.ID)
	err := d.authService.InvalidateToken(flow.ID)
	if err != nil {
		return err
	}
	return nil
}

func (d *ProviderDataPlane) noopHandler(context.Context, *dsdk.DataFlow) error {
	return nil
}

type ConnectionInvalidator interface {
	InvalidateConnection(processID string)
}
