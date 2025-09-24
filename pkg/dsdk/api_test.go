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

package dsdk

import (
	"net/url"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestParseIDFromURL(t *testing.T) {
	tests := []struct {
		name        string
		urlString   string
		expectedID  string
		expectError bool
	}{
		{
			name:        "valid URL with single path segment",
			urlString:   "http://example.com/12345",
			expectedID:  "12345",
			expectError: false,
		},
		{
			name:        "valid URL with multiple path segments",
			urlString:   "http://example.com/api/v1/flows/12345",
			expectedID:  "12345",
			expectError: false,
		},
		{
			name:        "valid URL with trailing slash",
			urlString:   "http://example.com/api/flows/12345/",
			expectedID:  "12345",
			expectError: false,
		},
		{
			name:        "valid URL with UUID",
			urlString:   "http://example.com/flows/123e4567-e89b-12d3-a456-426614174000",
			expectedID:  "123e4567-e89b-12d3-a456-426614174000",
			expectError: false,
		},
		{
			name:        "path with root slash only",
			urlString:   "http://example.com/",
			expectedID:  "",
			expectError: true,
		},
		{
			name:        "empty path",
			urlString:   "http://example.com",
			expectedID:  "",
			expectError: true,
		},
		{
			name:        "path with only slashes",
			urlString:   "http://example.com///",
			expectedID:  "",
			expectError: true,
		},
		{
			name:        "path with empty segments in middle",
			urlString:   "http://example.com/api//flows//12345",
			expectedID:  "12345",
			expectError: false,
		},
		{
			name:        "relative path",
			urlString:   "/api/flows/12345",
			expectedID:  "12345",
			expectError: false,
		},
		{
			name:        "just the ID",
			urlString:   "/12345",
			expectedID:  "12345",
			expectError: false,
		},
		{
			name:        "complex path with query parameters",
			urlString:   "http://example.com/api/flows/12345?param=value",
			expectedID:  "12345",
			expectError: false,
		},
		{
			name:        "path with fragment",
			urlString:   "http://example.com/api/flows/12345#section",
			expectedID:  "12345",
			expectError: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			parsedURL, err := url.Parse(tt.urlString)
			require.NoError(t, err, "Failed to parse test URL")

			id, err := ParseIDFromURL(parsedURL)

			if tt.expectError {
				assert.Error(t, err)
				assert.Empty(t, id)
			} else {
				assert.NoError(t, err)
				assert.Equal(t, tt.expectedID, id)
			}
		})
	}
}

func TestParseIDFromURL_NilURL(t *testing.T) {
	id, err := ParseIDFromURL(nil)

	assert.Error(t, err)
	assert.Empty(t, id)
	assert.Contains(t, err.Error(), "URL cannot be nil")
}

func TestParseIDFromURL_EdgeCases(t *testing.T) {
	t.Run("URL with port", func(t *testing.T) {
		u, err := url.Parse("http://example.com:8080/api/flows/12345")
		require.NoError(t, err)

		id, err := ParseIDFromURL(u)
		assert.NoError(t, err)
		assert.Equal(t, "12345", id)
	})

	t.Run("URL with special characters in ID", func(t *testing.T) {
		u, err := url.Parse("http://example.com/api/flows/test-id_123")
		require.NoError(t, err)

		id, err := ParseIDFromURL(u)
		assert.NoError(t, err)
		assert.Equal(t, "test-id_123", id)
	})

	t.Run("very long path", func(t *testing.T) {
		longPath := "/api/v1/dataplane/flows/processes/instances/executions/12345"
		u, err := url.Parse("http://example.com" + longPath)
		require.NoError(t, err)

		id, err := ParseIDFromURL(u)
		assert.NoError(t, err)
		assert.Equal(t, "12345", id)
	})
}
