// Copyright 2025 Nadrama Pty Ltd
// SPDX-License-Identifier: Apache-2.0

package s3lect

import (
	"context"
	"fmt"
	"time"
)

// Elector handles leader election for a distributed system
type Elector interface {
	// Start begins the leader election process
	Start(ctx context.Context) error

	// Stop stops the leader election process
	Stop() error

	// IsLeader returns true if this instance is currently the leader
	IsLeader() bool

	// WaitForLeadership blocks until this instance becomes leader
	WaitForLeadership(ctx context.Context) error

	// LeaderID returns the current leader's identity
	LeaderID() string

	// GetLeadershipStatus returns detailed leadership status
	GetLeadershipStatus() *LeadershipStatus

	// EnablePeerMode enables peer mode with the provided CA certificate
	EnablePeerMode(caCert []byte) error

	// UpdateConfig allows dynamic reconfiguration of the elector
	UpdateConfig(newConfig ElectorConfig) error

	// GetConfig returns the current configuration
	GetConfig() *ElectorConfig
}

// LeadershipStatus contains detailed information about current leadership
type LeadershipStatus struct {
	ServerID         string    `json:"serverID"`
	LockfilePath     string    `json:"lockfilePath"`
	IsLeader         bool      `json:"isLeader"`
	LeaderID         string    `json:"leaderID"`
	LeaderAddr       string    `json:"leaderAddr"`
	LeaderLastSeen   time.Time `json:"leaderLastSeen"`
	LastElectionTime time.Time `json:"lastElectionTime"`
	ConsecutiveFails int       `json:"consecutiveFails"`
}

// ElectorConfig contains configuration for leader election
type ElectorConfig struct {
	// LockfilePath is the S3 object key/path for the leader lockfile (e.g., "leader/my-group.json")
	LockfilePath string

	// ServerID is this instance's unique identifier
	ServerID string

	// ServerAddr is the address to advertise for peer health checks
	ServerAddr string

	// FrequentInterval is how often to check for leadership during transitions (default: 5s)
	FrequentInterval time.Duration

	// InfrequentInterval is how often to check for leadership during stable periods (default: 30s)
	InfrequentInterval time.Duration

	// LeaderTimeout is how long to wait before considering leader dead (default: 15s)
	LeaderTimeout time.Duration

	// PeerMode enables HTTP-based leader health checks to reduce S3 operations
	PeerMode bool

	// PeerTimeout is timeout for peer health check requests (default: 3s)
	PeerTimeout time.Duration

	// PeerCACert is the CA certificate for validating peer HTTPS connections
	PeerCACert []byte

	// OnAcquireLeadership is called after successfully claiming leadership in S3.
	// If this hook returns an error, leadership will be automatically resigned.
	// This is useful for critical operations like attaching network interfaces.
	OnAcquireLeadership func(ctx context.Context) error

	// OnLoseLeadership is called after losing leadership.
	// Errors are logged but do not prevent leadership transition (best effort cleanup).
	// This is useful for detaching network interfaces or stopping leader-only services.
	OnLoseLeadership func(ctx context.Context) error
}

// Validate validates the elector configuration
func (c *ElectorConfig) Validate() error {
	if c.LockfilePath == "" {
		return fmt.Errorf("lockfile path is required")
	}
	if c.ServerID == "" {
		return fmt.Errorf("server ID is required")
	}
	if c.ServerAddr == "" {
		return fmt.Errorf("server address is required")
	}
	if c.FrequentInterval == 0 {
		c.FrequentInterval = 5 * time.Second
	}
	if c.InfrequentInterval == 0 {
		c.InfrequentInterval = 30 * time.Second
	}
	if c.LeaderTimeout == 0 {
		c.LeaderTimeout = 15 * time.Second
	}
	if c.PeerTimeout == 0 {
		c.PeerTimeout = 3 * time.Second
	}
	return nil
}
