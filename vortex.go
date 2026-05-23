package analytics

import (
	"bytes"
	"context"
	"crypto/rand"
	"encoding/json"
	"fmt"
	"io"
	"log"
	"net/http"
	"net/url"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"time"
)

// Data structures
type ValueWrapper struct {
	Data string `json:"data"`
}

type TrackingData struct {
	Name       string `json:"name"`
	Value      string `json:"value"`
	Identity   string `json:"identity"`
	SessionID  string `json:"session_id"`
	Platform   string `json:"platform"`
	AppVersion string `json:"app_version"`
	Custom     string `json:"custom,omitempty"`
	Timestamp  string `json:"timestamp"`
}

type Tracking struct {
	TenantID string       `json:"tenant_id"`
	Tracking TrackingData `json:"tracking"`
}

type BatchedTracks struct {
	Tracks []Tracking `json:"tracks"`
}

type AnalyticsManager struct {
	mu sync.Mutex

	verbose bool

	tenantID          string
	serverURL         string
	platform          string
	autoBatching      bool
	autoFlushInterval time.Duration

	identity   string
	sessionID  string
	appVersion string
	customData string

	initialized     bool
	serverAlive     bool
	isServerChecked bool
	flushCompleted  bool

	internalQueue       []Tracking
	manualBatchedTracks []Tracking

	httpClient *http.Client
	cancelCtx  context.Context
	cancelFunc context.CancelFunc
}

// Singleton instance
var (
	instance *AnalyticsManager
	once     sync.Once
)

// Instance returns the singleton instance of the AnalyticsManager
func Instance() *AnalyticsManager {
	once.Do(func() {
		instance = &AnalyticsManager{
			serverURL:           "https://in.hintway.app",
			autoFlushInterval:   10 * time.Second,
			internalQueue:       make([]Tracking, 0),
			manualBatchedTracks: make([]Tracking, 0),
			httpClient: &http.Client{
				Timeout: 10 * time.Second,
			},
		}
	})
	return instance
}

// SetVerbose enables or disables verbose logging
func (m *AnalyticsManager) SetVerbose(verbose bool) {
	m.verbose = verbose
}

func (m *AnalyticsManager) hintwayLog(format string, args ...any) {
	if !m.verbose {
		return
	}
	msg := fmt.Sprintf(format, args...)
	log.Printf("[Hintway] %s\n", msg)
}

func (m *AnalyticsManager) Init(tenantID, serverURL, platform, appVersion string, autoBatching bool, flushIntervalSec int) {
	m.mu.Lock()
	if m.initialized {
		m.mu.Unlock()
		return
	}

	m.tenantID = tenantID
	m.serverURL = strings.TrimRight(serverURL, "/")
	m.platform = platform
	if appVersion != "" {
		m.appVersion = appVersion
	} else {
		m.appVersion = "1.0.0"
	}
	m.autoBatching = autoBatching
	if flushIntervalSec > 0 {
		m.autoFlushInterval = time.Duration(flushIntervalSec) * time.Second
	}

	m.cancelCtx, m.cancelFunc = context.WithCancel(context.Background())
	m.initialized = true
	m.mu.Unlock()

	m.hintwayLog("Init called: tenantId=%s, url=%s, platform=%s, appVersion=%s, autoBatching=%v, flushIntervalSec=%d",
		tenantID, serverURL, platform, appVersion, autoBatching, flushIntervalSec)

	m.initSession()
	m.hintwayLog("AnalyticsManager initialized")

	go m.checkServerAvailabilityAsync()

	m.TrackEvent("app_started", "")
}

func (m *AnalyticsManager) initSession() {
	m.identity = m.getPersistentIdentity()
	m.sessionID = generateUUID()
	m.hintwayLog("Session initialized - Identity: %s, SessionId: %s, AppVersion: %s", m.identity, m.sessionID, m.appVersion)
}

func (m *AnalyticsManager) getPersistentIdentity() string {
	execDir, err := os.Executable()
	var dir string
	if err == nil {
		dir = filepath.Dir(execDir)
	} else {
		dir = "."
	}
	path := filepath.Join(dir, "analytics.id")

	if data, err := os.ReadFile(path); err == nil && len(data) > 0 {
		id := string(data)
		m.hintwayLog("Loaded persistent identity: %s", id)
		return id
	}

	newID := generateUUID()
	if err := os.WriteFile(path, []byte(newID), 0644); err != nil {
		m.hintwayLog("Failed to write persistent identity: %v", err)
	} else {
		m.hintwayLog("Generated new persistent identity: %s", newID)
	}

	return newID
}

func (m *AnalyticsManager) createTracking(name, value string) Tracking {
	m.mu.Lock()
	customData := m.customData
	m.mu.Unlock()

	trackingData := TrackingData{
		Name:       name,
		Value:      value,
		Identity:   m.identity,
		SessionID:  m.sessionID,
		Platform:   m.platform,
		AppVersion: m.appVersion,
		Timestamp:  time.Now().UTC().Format(time.RFC3339),
	}

	if customData != "" {
		trackingData.Custom = customData
	}

	return Tracking{
		TenantID: m.tenantID,
		Tracking: trackingData,
	}
}

// Networking
func (m *AnalyticsManager) checkServerAvailabilityAsync() {
	if m.serverURL == "" {
		return
	}

	m.hintwayLog("Validating tenant at %s/validate?tenant_id=%s", m.serverURL, m.tenantID)

	ctx, cancel := context.WithTimeout(m.cancelCtx, 5*time.Second)
	defer cancel()

	reqURL := fmt.Sprintf("%s/validate?tenant_id=%s", m.serverURL, url.QueryEscape(m.tenantID))
	req, _ := http.NewRequestWithContext(ctx, http.MethodGet, reqURL, nil)

	resp, err := m.httpClient.Do(req)

	m.mu.Lock()
	if err == nil && resp.StatusCode >= 200 && resp.StatusCode < 300 {
		m.serverAlive = true
		m.hintwayLog("Tenant validation succeeded - tenant_id is valid")
	} else {
		m.serverAlive = false
		if err != nil {
			m.hintwayLog("Tenant validation request failed (server unreachable): %v", err)
		} else {
			body, _ := io.ReadAll(resp.Body)
			m.hintwayLog("Tenant validation failed - HTTP %d: %s", resp.StatusCode, strings.TrimSpace(string(body)))
		}
	}
	if resp != nil {
		resp.Body.Close()
	}

	m.isServerChecked = true
	alive := m.serverAlive
	autoBatching := m.autoBatching
	m.mu.Unlock()

	if alive {
		if autoBatching {
			m.startAutoFlush()
		} else {
			go m.flushInternalQueueAsync()
		}
	}
}

func (m *AnalyticsManager) sendRequest(ctx context.Context, endpoint string, data any) bool {
	jsonData, err := json.Marshal(data)
	if err != nil {
		m.hintwayLog("JSON serialization error: %v", err)
		return false
	}

	m.hintwayLog("Sending POST to %s%s with body: %s", m.serverURL, endpoint, string(jsonData))

	req, err := http.NewRequestWithContext(ctx, http.MethodPost, m.serverURL+endpoint, bytes.NewBuffer(jsonData))
	if err != nil {
		m.hintwayLog("Failed to create request: %v", err)
		return false
	}
	req.Header.Set("Content-Type", "application/json")

	resp, err := m.httpClient.Do(req)
	if err != nil {
		m.hintwayLog("Request exception: %v", err)
		return false
	}
	defer resp.Body.Close()

	if resp.StatusCode >= 200 && resp.StatusCode < 300 {
		m.hintwayLog("Request succeeded: %s%s", m.serverURL, endpoint)
		return true
	}

	body, _ := io.ReadAll(resp.Body)
	m.hintwayLog("Request failed: %s%s\nResponse code: %d\nResponse body: %s", m.serverURL, endpoint, resp.StatusCode, string(body))
	return false
}

// Flush logic
func (m *AnalyticsManager) startAutoFlush() {
	ticker := time.NewTicker(m.autoFlushInterval)
	go func() {
		for {
			select {
			case <-m.cancelCtx.Done():
				ticker.Stop()
				return
			case <-ticker.C:
				m.mu.Lock()
				alive := m.serverAlive
				m.mu.Unlock()
				if alive {
					m.flushInternalQueueAsync()
				}
			}
		}
	}()
}

func (m *AnalyticsManager) flushInternalQueueAsync() {
	m.mu.Lock()
	if len(m.internalQueue) == 0 {
		m.mu.Unlock()
		return
	}

	toSend := make([]Tracking, len(m.internalQueue))
	copy(toSend, m.internalQueue)
	m.internalQueue = m.internalQueue[:0]
	m.mu.Unlock()

	batch := BatchedTracks{Tracks: toSend}
	m.hintwayLog("Flushing internal queue with %d events", len(batch.Tracks))
	m.sendRequest(context.Background(), "/batch", batch)
}

func (m *AnalyticsManager) FlushManualBatch() {
	go func() {
		m.mu.Lock()
		if len(m.manualBatchedTracks) == 0 {
			m.mu.Unlock()
			return
		}

		toSend := make([]Tracking, len(m.manualBatchedTracks))
		copy(toSend, m.manualBatchedTracks)
		m.manualBatchedTracks = m.manualBatchedTracks[:0]
		m.mu.Unlock()

		batchToSend := BatchedTracks{Tracks: toSend}
		m.hintwayLog("Posting manual batch with %d events", len(batchToSend.Tracks))
		m.sendRequest(context.Background(), "/batch", batchToSend)
	}()
}

// Custom Data
func (m *AnalyticsManager) SetCustomData(customData map[string]any) {
	m.mu.Lock()
	defer m.mu.Unlock()

	if len(customData) == 0 {
		m.customData = ""
		return
	}

	jsonData, err := json.Marshal(customData)
	if err == nil {
		m.customData = string(jsonData)
	}
}

func (m *AnalyticsManager) ClearCustomData() {
	m.mu.Lock()
	m.customData = ""
	m.mu.Unlock()
}

func (m *AnalyticsManager) shouldSkip() bool {
	m.mu.Lock()
	defer m.mu.Unlock()
	return !m.serverAlive && m.isServerChecked && !m.autoBatching
}

// TrackEvent accepts either a string or a map representing props.
func (m *AnalyticsManager) TrackEvent(eventName string, props any) {
	if m.shouldSkip() {
		return
	}

	var jsonStr string
	switch v := props.(type) {
	case string:
		if v != "" {
			b, _ := json.Marshal(ValueWrapper{Data: v})
			jsonStr = string(b)
		}
	case map[string]any:
		b, _ := json.Marshal(v)
		jsonStr = string(b)
	}

	m.processTrackEvent(eventName, jsonStr)
}

func (m *AnalyticsManager) processTrackEvent(eventName, value string) {
	t := m.createTracking(eventName, value)
	m.hintwayLog("TrackEvent: %s value: %s", eventName, value)

	m.mu.Lock()
	defer m.mu.Unlock()

	if !m.isServerChecked || m.autoBatching {
		m.internalQueue = append(m.internalQueue, t)
		m.hintwayLog("Event queued internally. Queue size: %d", len(m.internalQueue))
	} else {
		// Non-blocking fire-and-forget
		go m.sendRequest(context.Background(), "/track", t)
	}
}

// BatchedTrackEvent accepts either a string or a map representing props.
func (m *AnalyticsManager) BatchedTrackEvent(eventName string, props any) {
	m.mu.Lock()
	alive := m.serverAlive
	m.mu.Unlock()

	if !alive {
		return
	}

	var jsonStr string
	switch v := props.(type) {
	case string:
		jsonStr = v
	case map[string]any:
		b, _ := json.Marshal(v)
		jsonStr = string(b)
	}

	tracking := m.createTracking(eventName, jsonStr)

	m.mu.Lock()
	m.manualBatchedTracks = append(m.manualBatchedTracks, tracking)
	m.hintwayLog("BatchedTrackEvent: %s added to manual batch. Batch size: %d", eventName, len(m.manualBatchedTracks))
	m.mu.Unlock()
}

// Shutdown flushes final queues and cancels background tasks.
func (m *AnalyticsManager) Shutdown() {
	m.mu.Lock()
	if m.flushCompleted {
		m.mu.Unlock()
		return
	}
	m.mu.Unlock()

	if m.cancelFunc != nil {
		m.cancelFunc()
	}

	m.mu.Lock()
	m.manualBatchedTracks = append(m.manualBatchedTracks, m.createTracking("app_exit", ""))

	if len(m.internalQueue) > 0 {
		m.manualBatchedTracks = append(m.manualBatchedTracks, m.internalQueue...)
		m.internalQueue = m.internalQueue[:0]
	}

	tracksToSend := make([]Tracking, len(m.manualBatchedTracks))
	copy(tracksToSend, m.manualBatchedTracks)
	m.mu.Unlock()

	if len(tracksToSend) > 0 {
		m.hintwayLog("Attempting final flush before exit with %d events", len(tracksToSend))
		batch := BatchedTracks{Tracks: tracksToSend}

		ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
		defer cancel()
		m.sendRequest(ctx, "/batch", batch)
	}

	m.mu.Lock()
	m.flushCompleted = true
	m.mu.Unlock()
}

// Helper utility
func generateUUID() string {
	b := make([]byte, 16)
	_, _ = rand.Read(b)
	return fmt.Sprintf("%x-%x-%x-%x-%x", b[0:4], b[4:6], b[6:8], b[8:10], b[10:])
}
