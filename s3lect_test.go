// Copyright 2025 Nadrama Pty Ltd
// SPDX-License-Identifier: Apache-2.0

package s3lect

import (
	"context"
	"log/slog"
	"os"
	"path/filepath"
	"testing"
	"time"
)

func TestS3Elector(t *testing.T) {
	// Create temp storage
	tempDir := filepath.Join(os.TempDir(), "s3lect-election-test")
	defer func() {
		_ = os.RemoveAll(tempDir)
	}()

	mockStorage, err := NewMockStorage(tempDir)
	if err != nil {
		t.Fatalf("Failed to create mock storage: %v", err)
	}

	logger := slog.New(slog.NewTextHandler(os.Stdout, &slog.HandlerOptions{Level: slog.LevelDebug}))

	t.Run("SingleInstanceBecomesLeader", func(t *testing.T) {
		config := &ElectorConfig{
			LockfilePath:     "test-locks/test-group.json",
			ServerID:         "instance-1",
			ServerAddr:       "test-instance-1:8443",
			FrequentInterval: 100 * time.Millisecond,
		}

		elector, err := NewS3Elector(S3ElectorOptions{
			Config:  config,
			Storage: mockStorage,
			Logger:  logger,
		})
		if err != nil {
			t.Fatalf("Failed to create elector: %v", err)
		}

		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()

		// Start the elector
		err = elector.Start(ctx)
		if err != nil {
			t.Fatalf("Failed to start elector: %v", err)
		}
		defer func() {
			_ = elector.Stop()
		}()

		// Wait for leadership
		err = elector.WaitForLeadership(ctx)
		if err != nil {
			t.Fatalf("Failed to acquire leadership: %v", err)
		}

		// Verify leadership
		if !elector.IsLeader() {
			t.Error("Expected to be leader")
		}

		if elector.LeaderID() != "instance-1" {
			t.Errorf("Expected leader ID to be 'instance-1', got '%s'", elector.LeaderID())
		}

		status := elector.GetLeadershipStatus()
		if !status.IsLeader {
			t.Error("Status should indicate leadership")
		}
		if status.LeaderID != "instance-1" {
			t.Error("Status should show correct leader ID")
		}
	})

	t.Run("LeaderElectionWithCompetition", func(t *testing.T) {
		// Clear any existing leader records
		if err := mockStorage.Cleanup(); err != nil {
			t.Fatalf("Failed to cleanup storage: %v", err)
		}

		config1 := &ElectorConfig{
			LockfilePath:     "test-locks/competition-group.json",
			ServerID:         "instance-1",
			ServerAddr:       "test-instance-1:8443",
			FrequentInterval: 50 * time.Millisecond,
		}

		config2 := &ElectorConfig{
			LockfilePath:     "test-locks/competition-group.json",
			ServerID:         "instance-2",
			ServerAddr:       "test-instance-2:8443",
			FrequentInterval: 50 * time.Millisecond,
		}

		elector1, err := NewS3Elector(S3ElectorOptions{
			Config:  config1,
			Storage: mockStorage,
			Logger:  logger,
		})
		if err != nil {
			t.Fatalf("Failed to create elector1: %v", err)
		}

		elector2, err := NewS3Elector(S3ElectorOptions{
			Config:  config2,
			Storage: mockStorage,
			Logger:  logger,
		})
		if err != nil {
			t.Fatalf("Failed to create elector2: %v", err)
		}

		ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel()

		// Start both electors
		if err := elector1.Start(ctx); err != nil {
			t.Fatalf("Failed to start elector1: %v", err)
		}
		defer func() {
			_ = elector1.Stop()
		}()

		if err := elector2.Start(ctx); err != nil {
			t.Fatalf("Failed to start elector2: %v", err)
		}
		defer func() {
			_ = elector2.Stop()
		}()

		// Wait a bit for the election to settle
		time.Sleep(500 * time.Millisecond)

		// Exactly one should be leader
		leaders := 0
		var leaderID string

		if elector1.IsLeader() {
			leaders++
			leaderID = elector1.LeaderID()
		}
		if elector2.IsLeader() {
			leaders++
			leaderID = elector2.LeaderID()
		}

		if leaders != 1 {
			t.Errorf("Expected exactly 1 leader, got %d", leaders)
		}

		// Both should agree on who the leader is
		if elector1.LeaderID() != elector2.LeaderID() {
			t.Error("Electors disagree on leader identity")
		}

		if elector1.LeaderID() != leaderID {
			t.Error("Leader ID mismatch")
		}
	})

	t.Run("LeaderFailover", func(t *testing.T) {
		// Clear any existing leader records
		if err := mockStorage.Cleanup(); err != nil {
			t.Fatalf("Failed to cleanup storage: %v", err)
		}

		config1 := &ElectorConfig{
			LockfilePath:     "test-locks/failover-group.json",
			ServerID:         "instance-1",
			ServerAddr:       "test-instance-1:8443",
			FrequentInterval: 100 * time.Millisecond,
			LeaderTimeout:    300 * time.Millisecond,
		}

		config2 := &ElectorConfig{
			LockfilePath:     "test-locks/failover-group.json",
			ServerID:         "instance-2",
			ServerAddr:       "test-instance-2:8443",
			FrequentInterval: 100 * time.Millisecond,
			LeaderTimeout:    300 * time.Millisecond,
		}

		elector1, err := NewS3Elector(S3ElectorOptions{
			Config:  config1,
			Storage: mockStorage,
			Logger:  logger,
		})
		if err != nil {
			t.Fatalf("Failed to create elector1: %v", err)
		}

		elector2, err := NewS3Elector(S3ElectorOptions{
			Config:  config2,
			Storage: mockStorage,
			Logger:  logger,
		})
		if err != nil {
			t.Fatalf("Failed to create elector2: %v", err)
		}

		ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
		defer cancel()

		// Start first elector
		if err := elector1.Start(ctx); err != nil {
			t.Fatalf("Failed to start elector1: %v", err)
		}

		// Wait for it to become leader
		time.Sleep(200 * time.Millisecond)

		if !elector1.IsLeader() {
			t.Fatal("Elector1 should be leader")
		}

		// Start second elector
		if err := elector2.Start(ctx); err != nil {
			t.Fatalf("Failed to start elector2: %v", err)
		}
		defer func() {
			_ = elector2.Stop()
		}()

		// Wait a bit - elector2 should remain follower
		time.Sleep(300 * time.Millisecond)

		if elector2.IsLeader() {
			t.Error("Elector2 should not be leader while elector1 is active")
		}

		// Stop elector1 (simulating failure)
		if err := elector1.Stop(); err != nil {
			t.Logf("Warning: failed to stop elector1: %v", err)
		}

		// Wait for failover to occur
		time.Sleep(1 * time.Second)

		// Elector2 should now be leader
		if !elector2.IsLeader() {
			t.Error("Elector2 should be leader after elector1 stopped")
		}

		if elector2.LeaderID() != "instance-2" {
			t.Error("Elector2 should recognize itself as leader")
		}
	})

	t.Run("ConfigValidation", func(t *testing.T) {
		// Test empty group
		config := &ElectorConfig{
			ServerID: "test",
		}
		err := config.Validate()
		if err == nil {
			t.Error("Expected validation error for empty group")
		}

		// Test empty ServerID
		config = &ElectorConfig{
			LockfilePath: "test-locks/test.json",
		}
		err = config.Validate()
		if err == nil {
			t.Error("Expected validation error for empty ServerID")
		}

		// Test valid config with defaults
		config = &ElectorConfig{
			LockfilePath: "test-locks/test.json",
			ServerID:     "test-id",
			ServerAddr:   "test:8443",
		}
		err = config.Validate()
		if err != nil {
			t.Errorf("Valid config should not have errors: %v", err)
		}

		if config.FrequentInterval == 0 {
			t.Error("Default frequent interval should be set")
		}
		if config.LeaderTimeout == 0 {
			t.Error("Default leader timeout should be set")
		}
	})
}

func TestS3ElectorHooks(t *testing.T) {
	tempDir := filepath.Join(os.TempDir(), "nstance-election-hooks-test")
	defer func() {
		_ = os.RemoveAll(tempDir)
	}()

	mockStorage, err := NewMockStorage(tempDir)
	if err != nil {
		t.Fatalf("Failed to create mock storage: %v", err)
	}

	logger := slog.New(slog.NewTextHandler(os.Stdout, &slog.HandlerOptions{Level: slog.LevelDebug}))

	t.Run("OnAcquireLeadershipSuccess", func(t *testing.T) {
		if err := mockStorage.Cleanup(); err != nil {
			t.Fatalf("Failed to cleanup storage: %v", err)
		}

		acquireCalled := make(chan bool, 1)

		config := &ElectorConfig{
			LockfilePath:     "test-locks/hooks-group-1.json",
			ServerID:         "hooks-instance-1",
			ServerAddr:       "test-hooks:8443",
			FrequentInterval: 100 * time.Millisecond,
			OnAcquireLeadership: func(ctx context.Context) error {
				acquireCalled <- true
				return nil
			},
		}

		elector, err := NewS3Elector(S3ElectorOptions{
			Config:  config,
			Storage: mockStorage,
			Logger:  logger,
		})
		if err != nil {
			t.Fatalf("Failed to create elector: %v", err)
		}

		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()

		err = elector.Start(ctx)
		if err != nil {
			t.Fatalf("Failed to start elector: %v", err)
		}
		defer func() {
			_ = elector.Stop()
		}()

		select {
		case <-acquireCalled:
		case <-time.After(2 * time.Second):
			t.Error("OnAcquireLeadership hook was not called")
		}

		if !elector.IsLeader() {
			t.Error("Expected to be leader after successful hook")
		}
	})

	t.Run("OnAcquireLeadershipFailureResigns", func(t *testing.T) {
		if err := mockStorage.Cleanup(); err != nil {
			t.Fatalf("Failed to cleanup storage: %v", err)
		}

		config := &ElectorConfig{
			LockfilePath:     "test-locks/hooks-group-2.json",
			ServerID:         "hooks-instance-2",
			ServerAddr:       "test-hooks-2:8443",
			FrequentInterval: 100 * time.Millisecond,
			OnAcquireLeadership: func(ctx context.Context) error {
				return context.Canceled
			},
		}

		elector, err := NewS3Elector(S3ElectorOptions{
			Config:  config,
			Storage: mockStorage,
			Logger:  logger,
		})
		if err != nil {
			t.Fatalf("Failed to create elector: %v", err)
		}

		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()

		err = elector.Start(ctx)
		if err != nil {
			t.Fatalf("Failed to start elector: %v", err)
		}
		defer func() {
			_ = elector.Stop()
		}()

		time.Sleep(1 * time.Second)

		if elector.IsLeader() {
			t.Error("Should not be leader after hook failure")
		}
	})

	t.Run("OnLoseLeadershipCalled", func(t *testing.T) {
		if err := mockStorage.Cleanup(); err != nil {
			t.Fatalf("Failed to cleanup storage: %v", err)
		}

		loseCalled := make(chan bool, 1)

		config1 := &ElectorConfig{
			LockfilePath:     "test-locks/hooks-group-3.json",
			ServerID:         "hooks-instance-3a",
			ServerAddr:       "test-hooks-3a:8443",
			FrequentInterval: 50 * time.Millisecond,
			LeaderTimeout:    200 * time.Millisecond,
			OnLoseLeadership: func(ctx context.Context) error {
				loseCalled <- true
				return nil
			},
		}

		config2 := &ElectorConfig{
			LockfilePath:     "test-locks/hooks-group-3.json",
			ServerID:         "hooks-instance-3b",
			ServerAddr:       "test-hooks-3b:8443",
			FrequentInterval: 50 * time.Millisecond,
			LeaderTimeout:    200 * time.Millisecond,
		}

		elector1, err := NewS3Elector(S3ElectorOptions{
			Config:  config1,
			Storage: mockStorage,
			Logger:  logger,
		})
		if err != nil {
			t.Fatalf("Failed to create elector1: %v", err)
		}

		elector2, err := NewS3Elector(S3ElectorOptions{
			Config:  config2,
			Storage: mockStorage,
			Logger:  logger,
		})
		if err != nil {
			t.Fatalf("Failed to create elector2: %v", err)
		}

		ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel()

		if err := elector1.Start(ctx); err != nil {
			t.Fatalf("Failed to start elector1: %v", err)
		}

		time.Sleep(200 * time.Millisecond)

		if !elector1.IsLeader() {
			t.Fatal("Elector1 should be leader")
		}

		if err := elector2.Start(ctx); err != nil {
			t.Fatalf("Failed to start elector2: %v", err)
		}
		defer func() {
			_ = elector2.Stop()
		}()

		if err := elector1.Stop(); err != nil {
			t.Logf("Warning: failed to stop elector1: %v", err)
		}

		select {
		case <-loseCalled:
		case <-time.After(2 * time.Second):
			t.Error("OnLoseLeadership hook was not called")
		}
	})
}
