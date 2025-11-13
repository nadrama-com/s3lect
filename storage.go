// Copyright 2025 Nadrama Pty Ltd
// SPDX-License-Identifier: Apache-2.0

package s3lect

import (
	"context"
	"errors"
)

var (
	ErrStorageNotFound     = errors.New("object not found")
	ErrStoragePrecondition = errors.New("precondition failed")
)

// Storage provides an abstraction for object storage operations
type Storage interface {
	// Get retrieves an object from storage and returns its contents and its etag
	// Returns ErrStorageNotFound if object does not exist
	Get(ctx context.Context, key string) (data []byte, etag string, err error)

	// PutIfMatch stores an object only if the ETag matches (empty string = must not exist)
	// Returns ErrStoragePrecondition if ETag doesn't match
	PutIfMatch(ctx context.Context, key string, data []byte, etag string) error
}
