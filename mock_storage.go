// Copyright 2025 Nadrama Pty Ltd
// SPDX-License-Identifier: Apache-2.0

package s3lect

import (
	"context"
	"crypto/md5"
	"fmt"
	"os"
	"path/filepath"
	"sync"
)

// MockStorage implements Storage interface using local filesystem for testing
type MockStorage struct {
	baseDir string
	mu      sync.RWMutex
}

// keyToPath converts a storage key to a file path
func (m *MockStorage) keyToPath(key string) string {
	return filepath.Join(m.baseDir, key)
}

// NewMockStorage creates a new mock storage instance
func NewMockStorage(baseDir string) (*MockStorage, error) {
	if err := os.MkdirAll(baseDir, 0755); err != nil {
		return nil, fmt.Errorf("failed to create base directory: %w", err)
	}

	return &MockStorage{
		baseDir: baseDir,
	}, nil
}

// NewMock creates a new mock storage instance with a temporary directory
func NewMock() *MockStorage {
	tempDir, err := os.MkdirTemp("", "mock-storage-")
	if err != nil {
		panic(fmt.Sprintf("failed to create temp dir for mock storage: %v", err))
	}

	storage, err := NewMockStorage(tempDir)
	if err != nil {
		panic(fmt.Sprintf("failed to create mock storage: %v", err))
	}

	return storage
}

// Get retrieves an object from mock storage
// Returns ErrStorageNotFound if object does not exist
func (m *MockStorage) Get(ctx context.Context, key string) ([]byte, string, error) {
	m.mu.RLock()
	defer m.mu.RUnlock()

	filePath := m.keyToPath(key)
	data, err := os.ReadFile(filePath)
	if os.IsNotExist(err) {
		return nil, "", ErrStorageNotFound
	}
	if err != nil {
		return nil, "", err
	}

	// Compute ETag as MD5 hash
	etag := fmt.Sprintf("%x", md5.Sum(data))
	return data, etag, nil
}

// PutIfMatch stores an object only if the ETag matches (optimistic locking)
// Empty etag means object must not exist
// Returns ErrStoragePrecondition if ETag doesn't match
func (m *MockStorage) PutIfMatch(ctx context.Context, key string, data []byte, etag string) error {
	m.mu.Lock()
	defer m.mu.Unlock()

	filePath := m.keyToPath(key)

	// Check current ETag
	currentETag := ""
	if _, err := os.Stat(filePath); err == nil {
		// File exists - get its ETag
		currentData, err := os.ReadFile(filePath)
		if err != nil {
			return fmt.Errorf("failed to read existing file: %w", err)
		}
		currentETag = fmt.Sprintf("%x", md5.Sum(currentData))
	}

	// Validate precondition
	if etag == "" {
		// Must not exist
		if currentETag != "" {
			return ErrStoragePrecondition
		}
	} else {
		// Must match
		if currentETag != etag {
			return ErrStoragePrecondition
		}
	}

	// Write file
	dir := filepath.Dir(filePath)
	if err := os.MkdirAll(dir, 0755); err != nil {
		return fmt.Errorf("failed to create directory: %w", err)
	}

	return os.WriteFile(filePath, data, 0644)
}

// Cleanup removes all files from mock storage
func (m *MockStorage) Cleanup() error {
	m.mu.Lock()
	defer m.mu.Unlock()

	// Remove all files in the base directory
	entries, err := os.ReadDir(m.baseDir)
	if err != nil {
		return err
	}

	for _, entry := range entries {
		path := filepath.Join(m.baseDir, entry.Name())
		if err := os.RemoveAll(path); err != nil {
			return err
		}
	}

	return nil
}
