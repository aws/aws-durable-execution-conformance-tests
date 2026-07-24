//go:build lambdacomponents.custom && (lambdacomponents.all || lambdacomponents.exporter.all || lambdacomponents.exporter.awss3)

// SPDX-FileCopyrightText: 2026-present Amazon.com, Inc. or its affiliates.
//
// SPDX-License-Identifier: Apache-2.0

package exporter

import (
	"github.com/open-telemetry/opentelemetry-collector-contrib/exporter/awss3exporter"
	"go.opentelemetry.io/collector/exporter"
)

func init() {
	Factories = append(Factories, func(_ string) exporter.Factory {
		return awss3exporter.NewFactory()
	})
}
