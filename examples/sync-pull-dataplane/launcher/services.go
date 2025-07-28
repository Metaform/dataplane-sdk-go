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

package launcher

import (
	"context"
	"github.com/metaform/dataplane-sdk-go/examples/sync-pull-dataplane/consumer"
	"github.com/metaform/dataplane-sdk-go/examples/sync-pull-dataplane/provider"
	"sync"
	"time"
)

func LaunchServicesAndWait(wg *sync.WaitGroup) {
	go func() {
		// Launch services
		providerDataPlane, consumerDataPlane := LaunchServices()

		wg.Wait()

		// Shutdown services
		ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
		defer cancel()
		consumerDataPlane.Shutdown(ctx)
		providerDataPlane.Shutdown(ctx)
	}()
}

func LaunchServices() (*provider.ProviderDataPlane, *consumer.ConsumerDataPlane) {
	providerDataPlane, err := provider.NewDataPlane()
	if err != nil {
		panic(err)
	}
	providerDataPlane.Init()

	consumerDataPlane, err := consumer.NewDataPlane()
	if err != nil {
		panic(err)
	}
	consumerDataPlane.Init()

	return providerDataPlane, consumerDataPlane
}
