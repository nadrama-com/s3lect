// Copyright 2025 Nadrama Pty Ltd
// SPDX-License-Identifier: Apache-2.0

// Package s3lect provides peer health server functionality for leader election.
//
// The health server can be used in two ways:
//
//  1. As a standalone HTTPS server:
//     server := s3lect.NewHealthServer(s3lect.HealthServerConfig{...})
//     server.Start(ctx)
//
//  2. As an HTTP handler embedded in existing servers:
//     mux.HandleFunc("/health/leadership", s3lect.NewLeadershipHandler(elector, logger))
package s3lect

import (
	"context"
	"crypto/tls"
	"encoding/json"
	"fmt"
	"log/slog"
	"net/http"
	"sync"
	"time"
)

// HealthServer provides the peer health endpoint for leader election
type HealthServer struct {
	server  *http.Server
	elector Elector
	logger  *slog.Logger

	mu      sync.RWMutex
	started bool
}

// HealthServerConfig contains configuration for the health server
type HealthServerConfig struct {
	BindAddress string
	Certificate tls.Certificate
	Elector     Elector
	Logger      *slog.Logger
}

// NewHealthServer creates a new standalone peer health server.
// This creates a dedicated HTTPS server for the leadership health endpoint.
//
// Example:
//
//	cert, _ := tls.X509KeyPair(certPEM, keyPEM)
//	server, err := s3lect.NewHealthServer(s3lect.HealthServerConfig{
//	    BindAddress:   "0.0.0.0:8993",
//	    Certificate:   cert,
//	    Elector:       elector,
//	    Logger:        logger,
//	})
//	server.Start(ctx)
func NewHealthServer(config HealthServerConfig) (*HealthServer, error) {
	if config.BindAddress == "" {
		return nil, fmt.Errorf("bind address is required")
	}
	if config.Elector == nil {
		return nil, fmt.Errorf("elector is required")
	}
	if config.Logger == nil {
		config.Logger = slog.Default()
	}

	// Create HTTP mux
	mux := http.NewServeMux()

	hs := &HealthServer{
		elector: config.Elector,
		logger:  config.Logger,
	}

	// Register health endpoint using the reusable handler
	mux.HandleFunc("/health/leadership", NewLeadershipHandler(config.Elector, config.Logger))

	// Create HTTPS server
	hs.server = &http.Server{
		Addr:    config.BindAddress,
		Handler: mux,
		TLSConfig: &tls.Config{
			Certificates: []tls.Certificate{config.Certificate},
			MinVersion:   tls.VersionTLS12,
		},
		ReadTimeout:  10 * time.Second,
		WriteTimeout: 10 * time.Second,
		IdleTimeout:  60 * time.Second,
	}

	return hs, nil
}

// Start starts the health server
func (hs *HealthServer) Start(ctx context.Context) error {
	hs.mu.Lock()
	defer hs.mu.Unlock()

	if hs.started {
		return fmt.Errorf("health server already started")
	}

	hs.started = true
	hs.logger.Info("Starting leader health server", "addr", hs.server.Addr)

	// Start server in goroutine
	go func() {
		if err := hs.server.ListenAndServeTLS("", ""); err != nil && err != http.ErrServerClosed {
			hs.logger.Error("Health server error", "error", err)
		}
	}()

	return nil
}

// Stop stops the health server
func (hs *HealthServer) Stop(ctx context.Context) error {
	hs.mu.Lock()
	defer hs.mu.Unlock()

	if !hs.started {
		return nil
	}

	hs.logger.Info("Stopping leader health server")

	// Shutdown with context timeout
	err := hs.server.Shutdown(ctx)
	hs.started = false
	return err
}

// NewLeadershipHandler creates an HTTP handler for embedding in existing servers.
// This allows you to add the leadership health endpoint to an existing HTTP server.
//
// Example:
//
//	mux := http.NewServeMux()
//	mux.HandleFunc("/health/leadership", s3lect.NewLeadershipHandler(elector, logger))
//	server := &http.Server{Handler: mux, ...}
func NewLeadershipHandler(elector Elector, logger *slog.Logger) http.HandlerFunc {
	if logger == nil {
		logger = slog.Default()
	}

	return func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodGet {
			http.Error(w, "Method not allowed", http.StatusMethodNotAllowed)
			return
		}

		// Check if this server is the leader
		if !elector.IsLeader() {
			logger.Debug("Health check request - not leader")
			http.Error(w, "Not leader", http.StatusNotFound)
			return
		}

		// Get leadership status
		status := elector.GetLeadershipStatus()
		if status == nil {
			logger.Error("Failed to get leadership status")
			http.Error(w, "Internal server error", http.StatusInternalServerError)
			return
		}

		// Create leader record response
		leaderRecord := LeaderRecord{
			LeaderID:    status.LeaderID,
			LeaderAddr:  status.LeaderAddr,
			LastUpdated: time.Now(),
		}

		// Set content type and return JSON
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusOK)

		if err := json.NewEncoder(w).Encode(leaderRecord); err != nil {
			logger.Error("Failed to encode response", "error", err)
			http.Error(w, "Internal server error", http.StatusInternalServerError)
			return
		}

		logger.Debug("Health check request - leader confirmed")
	}
}
