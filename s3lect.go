// Copyright 2025 Nadrama Pty Ltd
// SPDX-License-Identifier: Apache-2.0

package s3lect

import (
	"context"
	"crypto/tls"
	"crypto/x509"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"sync"
	"time"
)

// S3Elector implements leader election using S3 as the coordination mechanism
type S3Elector struct {
	config  *ElectorConfig
	storage Storage
	logger  *slog.Logger

	mu                sync.RWMutex
	isLeader          bool
	currentLeaderID   string
	currentLeaderAddr string
	lastLeaderSeen    time.Time
	lastElectionTime  time.Time
	consecutiveFails  int
	started           bool
	done              chan struct{}
	leadershipChan    chan bool
}

// LeaderRecord represents the leader information stored in S3
type LeaderRecord struct {
	LeaderID    string               `json:"leaderID,omitempty"`
	LeaderAddr  string               `json:"leaderAddr,omitempty"`
	LastUpdated time.Time            `json:"lastUpdated"`
	Metadata    LeaderRecordMetadata `json:"-"`
}

// LeaderRecordMetadata represents the S3 metadata of the LeaderRecord file
type LeaderRecordMetadata struct {
	ETag string
}

// S3ElectorOptions contains options for creating a new S3Elector
type S3ElectorOptions struct {
	Config  *ElectorConfig
	Storage Storage
	Logger  *slog.Logger
}

// NewS3Elector creates a new S3-based leader elector
func NewS3Elector(opts S3ElectorOptions) (*S3Elector, error) {
	if opts.Config == nil {
		return nil, fmt.Errorf("config is required")
	}
	if opts.Storage == nil {
		return nil, fmt.Errorf("storage is required")
	}
	if opts.Logger == nil {
		opts.Logger = slog.Default()
	}

	if err := opts.Config.Validate(); err != nil {
		return nil, fmt.Errorf("config validation failed: %w", err)
	}

	return &S3Elector{
		config:         opts.Config,
		storage:        opts.Storage,
		logger:         opts.Logger,
		done:           make(chan struct{}),
		leadershipChan: make(chan bool, 1),
	}, nil
}

// Start begins the leader election process
func (e *S3Elector) Start(ctx context.Context) error {
	e.mu.Lock()
	defer e.mu.Unlock()

	if e.started {
		return fmt.Errorf("elector already started")
	}

	e.started = true
	e.logger.Info("Starting leader election", "lockfilePath", e.config.LockfilePath, "serverID", e.config.ServerID, "serverAddr", e.config.ServerAddr)

	// Start the election loop
	go e.electionLoop(ctx)

	return nil
}

// Stop stops the leader election process
func (e *S3Elector) Stop() error {
	e.mu.Lock()
	defer e.mu.Unlock()

	if !e.started {
		return nil
	}

	e.logger.Info("Stopping leader election", "lockfilePath", e.config.LockfilePath)
	close(e.done)
	e.started = false

	// If we were the leader, try to clean up our lock
	if e.isLeader {
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		e.resignLeadership(ctx)
	}

	return nil
}

// IsLeader returns true if this instance is currently the leader
func (e *S3Elector) IsLeader() bool {
	e.mu.RLock()
	defer e.mu.RUnlock()
	return e.isLeader
}

// WaitForLeadership blocks until this instance becomes leader
func (e *S3Elector) WaitForLeadership(ctx context.Context) error {
	if e.IsLeader() {
		return nil
	}

	select {
	case <-ctx.Done():
		return ctx.Err()
	case isLeader := <-e.leadershipChan:
		if isLeader {
			return nil
		}
		return fmt.Errorf("lost leadership while waiting")
	}
}

// LeaderID returns the current leader's identity
func (e *S3Elector) LeaderID() string {
	e.mu.RLock()
	defer e.mu.RUnlock()
	return e.currentLeaderID
}

// GetLeadershipStatus returns detailed leadership status
func (e *S3Elector) GetLeadershipStatus() *LeadershipStatus {
	e.mu.RLock()
	defer e.mu.RUnlock()

	return &LeadershipStatus{
		IsLeader:         e.isLeader,
		LeaderID:         e.currentLeaderID,
		LeaderAddr:       e.currentLeaderAddr,
		LeaderLastSeen:   e.lastLeaderSeen,
		LockfilePath:     e.config.LockfilePath,
		ServerID:         e.config.ServerID,
		LastElectionTime: e.lastElectionTime,
		ConsecutiveFails: e.consecutiveFails,
	}
}

// electionLoop runs the main leader election loop
func (e *S3Elector) electionLoop(ctx context.Context) {
	// Start with frequent interval
	ticker := time.NewTicker(e.config.FrequentInterval)
	defer ticker.Stop()

	// Perform initial election check
	e.performElectionCycle(ctx)

	for {
		select {
		case <-ctx.Done():
			e.logger.Info("Election loop stopped due to context cancellation")
			return
		case <-e.done:
			e.logger.Info("Election loop stopped")
			return
		case <-ticker.C:
			e.performElectionCycle(ctx)
		}
	}
}

// performElectionCycle performs one cycle of the election algorithm
func (e *S3Elector) performElectionCycle(ctx context.Context) {
	lockKey := e.config.LockfilePath

	e.mu.RLock()
	isLeader := e.isLeader
	cachedLeaderAddr := e.currentLeaderAddr
	e.mu.RUnlock()

	e.logger.Debug("Performing election cycle", "lockKey", lockKey, "isLeader", isLeader)

	var leaderRecord *LeaderRecord
	var err error

	// Try peer communication first for followers in peer mode
	if !isLeader && e.config.PeerMode && cachedLeaderAddr != "" {
		// Try peer health check using cached leader address
		peerRecord, peerErr := e.tryPeerHealthCheck(ctx, cachedLeaderAddr)
		if peerErr == nil {
			// Peer check succeeded, use peer data (no S3 read needed!)
			leaderRecord = peerRecord
			e.logger.Debug("Used peer health check", "leaderAddress", cachedLeaderAddr)
		} else {
			// Peer check failed, fall back to S3
			e.logger.Debug("Peer health check failed, falling back to S3", "error", peerErr)
			leaderRecord, err = e.readLeaderRecord(ctx, lockKey)
		}
	} else {
		// Leaders, non-peer mode, or no cached address - always use S3
		leaderRecord, err = e.readLeaderRecord(ctx, lockKey)
	}

	if err != nil {
		e.logger.Warn("Failed to read leader record", "error", err)
		e.incrementFailureCount()
		return
	}

	now := time.Now()

	// Update cached leader information
	if leaderRecord != nil {
		e.mu.Lock()
		e.currentLeaderID = leaderRecord.LeaderID
		e.currentLeaderAddr = leaderRecord.LeaderAddr
		e.lastLeaderSeen = leaderRecord.LastUpdated
		e.mu.Unlock()

		timeSinceUpdate := now.Sub(leaderRecord.LastUpdated)

		// If the leader is still active and it's us, update our record
		if leaderRecord.LeaderID == e.config.ServerID && timeSinceUpdate < e.config.LeaderTimeout {
			if err := e.updateLeaderRecord(ctx, lockKey, e.config.ServerID, leaderRecord.Metadata.ETag); err != nil {
				e.logger.Error("Failed to update leader record", "error", err)
				e.incrementFailureCount()
				return
			}

			e.mu.Lock()
			if !e.isLeader {
				e.isLeader = true
				e.logger.Info("Confirmed leadership")
				e.notifyLeadershipChange(true)
			}
			e.consecutiveFails = 0
			e.mu.Unlock()
			return
		}

		// If the leader is still active but not us, we're a follower
		if timeSinceUpdate < e.config.LeaderTimeout {
			var shouldCallHook bool

			e.mu.Lock()
			if e.isLeader {
				e.isLeader = false
				shouldCallHook = true
			}
			e.consecutiveFails = 0
			e.mu.Unlock()

			if shouldCallHook {
				e.logger.Info("Lost leadership", "currentLeader", leaderRecord.LeaderID)

				// Call OnLoseLeadership hook (outside lock)
				if e.config.OnLoseLeadership != nil {
					if err := e.config.OnLoseLeadership(ctx); err != nil {
						e.logger.Warn("OnLoseLeadership hook failed", "error", err)
					}
				}

				e.notifyLeadershipChange(false)
			}
			return
		}

		e.logger.Info("Leader appears inactive", "leaderID", leaderRecord.LeaderID, "timeSinceUpdate", timeSinceUpdate)
	}

	// Cache wasLeader state for use after attemptLeadership
	e.mu.RLock()
	wasLeader := e.isLeader
	e.mu.RUnlock()

	// Step 3: No active leader, attempt to become leader
	if err := e.attemptLeadership(ctx, lockKey, leaderRecord); err != nil {
		e.logger.Warn("Failed to acquire leadership", "error", err)
		e.incrementFailureCount()

		// If we thought we were leader but failed to update, we're no longer leader
		if wasLeader {
			e.mu.Lock()
			e.isLeader = false
			e.mu.Unlock()
			e.logger.Info("Lost leadership due to update failure")

			// Call OnLoseLeadership hook
			if e.config.OnLoseLeadership != nil {
				if err := e.config.OnLoseLeadership(ctx); err != nil {
					e.logger.Warn("OnLoseLeadership hook failed", "error", err)
				}
			}

			e.notifyLeadershipChange(false)
		}
		return
	}

	// Successfully became/remained leader
	e.mu.Lock()
	becameLeader := !e.isLeader
	if becameLeader {
		e.isLeader = true
		e.lastElectionTime = now
	}
	e.currentLeaderID = e.config.ServerID
	e.currentLeaderAddr = e.config.ServerAddr
	e.lastLeaderSeen = now
	e.consecutiveFails = 0
	e.mu.Unlock()

	if becameLeader {
		e.logger.Info("Acquired leadership", "lockfilePath", e.config.LockfilePath)

		// Call OnAcquireLeadership hook
		if e.config.OnAcquireLeadership != nil {
			if err := e.config.OnAcquireLeadership(ctx); err != nil {
				e.logger.Error("OnAcquireLeadership hook failed, resigning leadership", "error", err)

				// Resign leadership immediately
				e.mu.Lock()
				e.isLeader = false
				e.lastElectionTime = time.Time{}
				e.mu.Unlock()

				// Call OnLoseLeadership to clean up
				if e.config.OnLoseLeadership != nil {
					if cleanupErr := e.config.OnLoseLeadership(ctx); cleanupErr != nil {
						e.logger.Warn("OnLoseLeadership hook failed during resignation cleanup", "error", cleanupErr)
					}
				}

				// Mark leader record as resigned to allow others to take over
				record, readErr := e.readLeaderRecord(ctx, lockKey)
				if readErr == nil && record != nil && record.LeaderID == e.config.ServerID {
					record.LastUpdated = time.Time{}
					data, marshalErr := json.Marshal(record)
					if marshalErr == nil {
						if putErr := e.storage.PutIfMatch(ctx, lockKey, data, record.Metadata.ETag); putErr != nil {
							e.logger.Warn("Failed to update leader record during resignation", "error", putErr)
						}
					}
				}

				return
			}
		}

		e.notifyLeadershipChange(true)
	}
}

// readLeaderRecord reads the current leader record from storage
func (e *S3Elector) readLeaderRecord(ctx context.Context, key string) (*LeaderRecord, error) {
	// read
	data, etag, err := e.storage.Get(ctx, key)
	if errors.Is(err, ErrStorageNotFound) {
		return nil, nil // No leader record exists yet
	} else if err != nil {
		return nil, fmt.Errorf("failed to read leader record: %w", err)
	}

	// unmarshal
	var record LeaderRecord
	if err := json.Unmarshal(data, &record); err != nil {
		return nil, fmt.Errorf("failed to unmarshal leader record: %w", err)
	}
	record.Metadata.ETag = etag

	return &record, nil
}

// updateLeaderRecord updates the leader record in storage using conditional write
func (e *S3Elector) updateLeaderRecord(ctx context.Context, key, leaderID string, etag string) error {
	record := &LeaderRecord{
		LeaderID:    leaderID,
		LeaderAddr:  e.config.ServerAddr,
		LastUpdated: time.Now(),
	}

	data, err := json.Marshal(record)
	if err != nil {
		return fmt.Errorf("failed to marshal leader record: %w", err)
	}

	return e.storage.PutIfMatch(ctx, key, data, etag)
}

// attemptLeadership attempts to acquire leadership through conditional writes
func (e *S3Elector) attemptLeadership(ctx context.Context, key string, currentRecord *LeaderRecord) error {
	// Get the ETag from current record if it exists, empty string if not
	etag := ""
	if currentRecord != nil {
		etag = currentRecord.Metadata.ETag
	}

	// Create new leader record with conditional write
	return e.updateLeaderRecord(ctx, key, e.config.ServerID, etag)
}

// resignLeadership attempts to resign from leadership
func (e *S3Elector) resignLeadership(ctx context.Context) {
	lockKey := e.config.LockfilePath

	// Call OnLoseLeadership hook first
	if e.config.OnLoseLeadership != nil {
		if err := e.config.OnLoseLeadership(ctx); err != nil {
			e.logger.Warn("OnLoseLeadership hook failed during resignation", "error", err)
		}
	}

	// Read current record to get ETag
	record, err := e.readLeaderRecord(ctx, lockKey)
	if err != nil {
		e.logger.Warn("Failed to read leader record during resignation", "error", err)
		return
	}

	// Check that the current leader in the record is this server
	if record.LeaderID != e.config.ServerID {
		e.logger.Warn("Failed to resign from leadership - no longer the current leader")
		return
	}

	// Mark as resigned by setting timestamp to zero
	record.LastUpdated = time.Time{}
	data, err := json.Marshal(record)
	if err != nil {
		e.logger.Warn("Failed to marshal resignation record", "error", err)
		return
	}
	if err := e.storage.PutIfMatch(ctx, lockKey, data, record.Metadata.ETag); err != nil {
		e.logger.Warn("Failed to update leader record during resignation", "error", err)
	} else {
		e.logger.Info("Successfully resigned from leadership")
	}
}

// incrementFailureCount increments the consecutive failure count
func (e *S3Elector) incrementFailureCount() {
	e.mu.Lock()
	defer e.mu.Unlock()
	e.consecutiveFails++
}

// notifyLeadershipChange notifies about leadership changes
func (e *S3Elector) notifyLeadershipChange(isLeader bool) {
	// Send non-blocking notification to leadership channel
	select {
	case e.leadershipChan <- isLeader:
	default:
		// Channel is full, skip
	}
}

// EnablePeerMode enables peer mode with the provided CA certificate
func (e *S3Elector) EnablePeerMode(caCert []byte) error {
	e.mu.Lock()
	defer e.mu.Unlock()

	e.config.PeerMode = true
	e.config.PeerCACert = caCert
	e.logger.Info("Peer mode enabled for cost optimization")
	return nil
}

// UpdateConfig allows dynamic reconfiguration of the elector
func (e *S3Elector) UpdateConfig(newConfig ElectorConfig) error {
	if err := newConfig.Validate(); err != nil {
		return fmt.Errorf("config validation failed: %w", err)
	}

	e.mu.Lock()
	defer e.mu.Unlock()

	e.config = &newConfig
	e.logger.Info("Configuration updated", "lockfilePath", newConfig.LockfilePath, "serverID", newConfig.ServerID)
	return nil
}

// GetConfig returns the current configuration
func (e *S3Elector) GetConfig() *ElectorConfig {
	e.mu.RLock()
	defer e.mu.RUnlock()

	// Return a copy to prevent external modification
	configCopy := *e.config
	return &configCopy
}

// tryPeerHealthCheck attempts to check leader health via peer communication
func (e *S3Elector) tryPeerHealthCheck(ctx context.Context, leaderAddress string) (*LeaderRecord, error) {
	if !e.config.PeerMode || leaderAddress == "" {
		return nil, fmt.Errorf("peer mode not enabled or no leader address")
	}

	return e.retryOperation(ctx, func() (*LeaderRecord, error) {
		return e.performPeerHealthCheck(ctx, leaderAddress)
	})
}

// performPeerHealthCheck makes an HTTPS request to the leader health endpoint
func (e *S3Elector) performPeerHealthCheck(ctx context.Context, leaderAddress string) (*LeaderRecord, error) {
	// Create HTTP client with CA certificate validation if available
	client := &http.Client{
		Timeout: e.config.PeerTimeout,
	}

	if e.config.PeerCACert != nil {
		// Create CA certificate pool
		caCertPool := x509.NewCertPool()
		if !caCertPool.AppendCertsFromPEM(e.config.PeerCACert) {
			return nil, fmt.Errorf("failed to parse CA certificate")
		}

		// Configure TLS with CA verification
		client.Transport = &http.Transport{
			TLSClientConfig: &tls.Config{
				RootCAs:    caCertPool,
				MinVersion: tls.VersionTLS12,
			},
		}
	}

	// Construct URL (assume HTTPS)
	url := fmt.Sprintf("https://%s/health/leadership", leaderAddress)

	// Create request with context
	req, err := http.NewRequestWithContext(ctx, "GET", url, nil)
	if err != nil {
		return nil, fmt.Errorf("failed to create request: %w", err)
	}

	// Make the request
	resp, err := client.Do(req)
	if err != nil {
		return nil, fmt.Errorf("request failed: %w", err)
	}
	defer func() { _ = resp.Body.Close() }()

	// Check status code
	if resp.StatusCode != http.StatusOK {
		return nil, fmt.Errorf("leader returned status %d", resp.StatusCode)
	}

	// Read and parse response
	body, err := io.ReadAll(resp.Body)
	if err != nil {
		return nil, fmt.Errorf("failed to read response: %w", err)
	}

	var leaderRecord LeaderRecord
	if err := json.Unmarshal(body, &leaderRecord); err != nil {
		return nil, fmt.Errorf("failed to parse response: %w", err)
	}

	return &leaderRecord, nil
}

// retryOperation performs an operation with retry logic (immediate, immediate, 1s delay)
func (e *S3Elector) retryOperation(ctx context.Context, operation func() (*LeaderRecord, error)) (*LeaderRecord, error) {
	// Attempt 1
	if result, err := operation(); err == nil {
		return result, nil
	}

	// Attempt 2 (immediate)
	if result, err := operation(); err == nil {
		return result, nil
	}

	// Attempt 3 (after 1s delay)
	select {
	case <-time.After(1 * time.Second):
		return operation()
	case <-ctx.Done():
		return nil, ctx.Err()
	}
}
