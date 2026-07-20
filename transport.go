// Copyright (C) 2026 desk.ly GmbH
// SPDX-License-Identifier: AGPL-3.0-or-later

package redistransport

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"log/slog"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/dunglas/mercure"
	"github.com/redis/go-redis/v9"
)

type RedisTransport struct {
	subscribers  *mercure.SubscriberList
	logger       *slog.Logger
	client       *redis.Client
	stream       string
	channel      string
	indexKey     string // Redis Hash mapping Mercure event UUIDs to stream IDs for O(1) lookups
	instanceID   string // Filters own Pub/Sub messages to prevent double-dispatch to local subscribers
	size         int64
	historyLimit int64 // Max stream entries read per history replay. 0 disables replay (fast-forward only)
	closed       chan struct{}
	closedOnce   sync.Once
	cancel       context.CancelFunc

	mutex       sync.RWMutex
	lastEventID string
}

type pubsubMessage struct {
	InstanceID string          `json:"instance_id"`
	Update     *mercure.Update `json:"update"`
}

const (
	trimInterval       = 10 * time.Second
	pubsubReconnectMin = 500 * time.Millisecond
	pubsubReconnectMax = 30 * time.Second
)

func NewRedisTransport(
	subscriberList *mercure.SubscriberList,
	logger *slog.Logger,
	url string,
	stream string,
	size int64,
	historyLimit int64,
) (*RedisTransport, error) {
	options, err := redis.ParseURL(url)
	if err != nil {
		return nil, fmt.Errorf("invalid Redis URL %q: %w", url, err)
	}

	client := redis.NewClient(options)

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	if err := client.Ping(ctx).Err(); err != nil {
		return nil, fmt.Errorf("unable to connect to Redis: %w", err)
	}

	if stream == "" {
		stream = "mercure"
	}

	listenCtx, listenCancel := context.WithCancel(context.Background())

	t := &RedisTransport{
		subscribers:  subscriberList,
		logger:       logger,
		client:       client,
		stream:       stream,
		channel:      stream + ":pubsub",
		indexKey:     stream + ":index",
		instanceID:   generateInstanceID(),
		size:         size,
		historyLimit: historyLimit,
		closed:       make(chan struct{}),
		cancel:       listenCancel,
		lastEventID:  mercure.EarliestLastEventID,
	}

	t.lastEventID = t.fetchLastEventID()
	go t.listenPubSub(listenCtx)
	go t.trimLoop(listenCtx)

	return t, nil
}

func generateInstanceID() string {
	randomBytes := make([]byte, 16)
	rand.Read(randomBytes) //nolint:errcheck
	return hex.EncodeToString(randomBytes)
}

func (t *RedisTransport) fetchLastEventID() string {
	messages, err := t.client.XRevRangeN(context.Background(), t.stream, "+", "-", 1).Result()
	if err != nil || len(messages) == 0 {
		return mercure.EarliestLastEventID
	}

	data, ok := messages[0].Values["data"].(string)
	if !ok {
		return mercure.EarliestLastEventID
	}

	var update mercure.Update
	if err := json.Unmarshal([]byte(data), &update); err != nil {
		return mercure.EarliestLastEventID
	}

	return update.ID
}

// listenPubSub reconnects automatically on disconnect so cross-instance event
// distribution survives transient Redis failures (network blips, failovers).
func (t *RedisTransport) listenPubSub(ctx context.Context) {
	backoff := pubsubReconnectMin

	for {
		select {
		case <-ctx.Done():
			return
		default:
		}

		wasHealthy := t.subscribePubSub(ctx)

		select {
		case <-ctx.Done():
			return
		case <-time.After(backoff):
			t.logger.LogAttrs(ctx, slog.LevelWarn, "Redis Pub/Sub disconnected, reconnecting",
				slog.Duration("backoff", backoff))
		}

		// Replay events that landed in the stream while pub/sub was disconnected,
		// so existing subscribers don't miss cross-instance dispatches.
		t.replayMissedEvents(ctx)

		if wasHealthy {
			backoff = pubsubReconnectMin
		} else {
			backoff = min(backoff*2, pubsubReconnectMax)
		}
	}
}

// replayMissedEvents dispatches events that landed in the stream after lastEventID
// to local subscribers. The anchor is resolved via the index in O(1). If it can no
// longer be anchored (trimmed), it fast-forwards to the latest entry rather than
// reading the whole stream to locate it.
func (t *RedisTransport) replayMissedEvents(ctx context.Context) {
	t.mutex.RLock()
	replayAfterID := t.lastEventID
	t.mutex.RUnlock()

	if replayAfterID == mercure.EarliestLastEventID {
		return
	}

	cursor, found := t.resolveStreamCursor(ctx, replayAfterID)
	if !found {
		t.fastForwardLastEventID(ctx, replayAfterID)
		return
	}

	for {
		messages, err := t.client.XRangeN(ctx, t.stream, cursor, "+", historyPageSize).Result()
		if err != nil {
			t.logger.LogAttrs(ctx, slog.LevelError, "Failed to replay missed events from stream", slog.Any("error", err))
			return
		}

		if len(messages) == 0 {
			break
		}

		for _, message := range messages {
			data, ok := message.Values["data"].(string)
			if !ok {
				continue
			}

			var update mercure.Update
			if err := json.Unmarshal([]byte(data), &update); err != nil {
				continue
			}

			t.mutex.Lock()
			t.lastEventID = update.ID
			t.mutex.Unlock()

			for _, subscriber := range t.subscribers.MatchAny(&update) {
				subscriber.Dispatch(ctx, &update, false)
			}
		}

		if int64(len(messages)) < historyPageSize {
			break
		}

		cursor = "(" + messages[len(messages)-1].ID
	}
}

// fastForwardLastEventID advances lastEventID to the latest stream entry without
// dispatching, used when the previous anchor is no longer present in the stream.
func (t *RedisTransport) fastForwardLastEventID(ctx context.Context, missingID string) {
	t.logger.LogAttrs(ctx, slog.LevelWarn, "lastEventID not found in stream (likely trimmed), fast-forwarding to latest",
		slog.String("lastEventID", missingID))

	latestEventID := t.fetchLastEventID()
	if latestEventID == mercure.EarliestLastEventID {
		return
	}

	t.mutex.Lock()
	t.lastEventID = latestEventID
	t.mutex.Unlock()
}

// subscribePubSub returns true if at least one message was received (= healthy connection).
func (t *RedisTransport) subscribePubSub(ctx context.Context) (received bool) {
	pubsub := t.client.Subscribe(ctx, t.channel)
	defer pubsub.Close()

	// A single malformed message must not crash the long-lived listener goroutine.
	// recover keeps the transport running.
	defer func() {
		if r := recover(); r != nil {
			t.logger.LogAttrs(ctx, slog.LevelError, "Recovered from panic in Redis Pub/Sub listener", slog.Any("panic", r))
		}
	}()

	pubsubChannel := pubsub.Channel()

	for {
		select {
		case <-ctx.Done():
			return received
		case redisMessage, ok := <-pubsubChannel:
			if !ok {
				return received
			}

			received = true

			var message pubsubMessage
			if err := json.Unmarshal([]byte(redisMessage.Payload), &message); err != nil {
				t.logger.LogAttrs(ctx, slog.LevelWarn, "Unable to unmarshal Redis Pub/Sub message, skipping", slog.Any("error", err))
				continue
			}

			if message.InstanceID == t.instanceID {
				continue
			}

			// The legitimate path always sets Update. A nil here means a foreign or
			// malformed PUBLISH (missing field or "update":null) that would otherwise nil-deref below.
			if message.Update == nil {
				t.logger.LogAttrs(ctx, slog.LevelWarn, "Received Redis Pub/Sub message without update payload, skipping", slog.String("instance_id", message.InstanceID))
				continue
			}

			t.mutex.Lock()
			t.lastEventID = message.Update.ID
			t.mutex.Unlock()

			for _, subscriber := range t.subscribers.MatchAny(message.Update) {
				subscriber.Dispatch(ctx, message.Update, false)
			}
		}
	}
}

const historyPageSize int64 = 1000

func (t *RedisTransport) resolveStreamCursor(ctx context.Context, eventID string) (cursor string, found bool) {
	streamID, err := t.client.HGet(ctx, t.indexKey, eventID).Result()
	if err != nil {
		return "-", false
	}

	entries, err := t.client.XRangeN(ctx, t.stream, streamID, streamID, 1).Result()
	if err != nil || len(entries) == 0 {
		t.client.HDel(ctx, t.indexKey, eventID) //nolint:errcheck
		return "-", false
	}

	return "(" + streamID, true
}

func (t *RedisTransport) cleanupIndex(ctx context.Context) {
	oldest, err := t.client.XRangeN(ctx, t.stream, "-", "+", 1).Result()
	if err != nil || len(oldest) == 0 {
		return
	}
	oldestStreamID := oldest[0].ID

	var scanCursor uint64
	for {
		keys, nextCursor, err := t.client.HScan(ctx, t.indexKey, scanCursor, "*", 100).Result()
		if err != nil {
			return
		}

		var stale []string
		for i := 0; i+1 < len(keys); i += 2 {
			streamID := keys[i+1]
			if compareStreamIDs(streamID, oldestStreamID) < 0 {
				stale = append(stale, keys[i])
			}
		}

		if len(stale) > 0 {
			t.client.HDel(ctx, t.indexKey, stale...) //nolint:errcheck
		}

		scanCursor = nextCursor
		if scanCursor == 0 {
			break
		}
	}
}

// trimLoop bounds the stream and index on a timer. Trimming on a schedule (rather
// than per write) keeps it independent of per-instance write counts and process
// restarts, which matters because the stream is shared across all instances.
func (t *RedisTransport) trimLoop(ctx context.Context) {
	ticker := time.NewTicker(trimInterval)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			t.trim(ctx)
		}
	}
}

// trim caps the stream at the configured size using an exact MAXLEN (not approximate,
// so the bound holds precisely) and drops index entries for evicted events.
func (t *RedisTransport) trim(ctx context.Context) {
	if t.size <= 0 {
		return
	}

	if err := t.client.XTrimMaxLen(ctx, t.stream, t.size).Err(); err != nil {
		t.logger.LogAttrs(ctx, slog.LevelError, "Failed to trim Redis Stream", slog.Any("error", err))
		return
	}

	t.cleanupIndex(ctx)
}

func compareStreamIDs(a, b string) int {
	aTimestamp, aSequence := parseStreamID(a)
	bTimestamp, bSequence := parseStreamID(b)

	if aTimestamp != bTimestamp {
		if aTimestamp < bTimestamp {
			return -1
		}
		return 1
	}
	if aSequence != bSequence {
		if aSequence < bSequence {
			return -1
		}
		return 1
	}
	return 0
}

func parseStreamID(id string) (int64, int64) {
	parts := strings.SplitN(id, "-", 2)
	timestamp, _ := strconv.ParseInt(parts[0], 10, 64)
	var sequence int64
	if len(parts) > 1 {
		sequence, _ = strconv.ParseInt(parts[1], 10, 64)
	}
	return timestamp, sequence
}

func (t *RedisTransport) Dispatch(ctx context.Context, update *mercure.Update) error {
	select {
	case <-t.closed:
		return mercure.ErrClosedTransport
	default:
	}

	update.AssignUUID()

	updateJSON, err := json.Marshal(*update)
	if err != nil {
		return fmt.Errorf("error when marshaling update: %w", err)
	}

	streamID, err := t.client.XAdd(ctx, &redis.XAddArgs{
		Stream: t.stream,
		Values: map[string]interface{}{"data": string(updateJSON)},
	}).Result()
	if err != nil {
		return fmt.Errorf("error adding to Redis Stream: %w", err)
	}

	// Index event UUID → stream ID for O(1) lookups in replay/history.
	t.client.HSet(ctx, t.indexKey, update.ID, streamID) //nolint:errcheck

	pubsubPayload, err := json.Marshal(pubsubMessage{InstanceID: t.instanceID, Update: update})
	if err != nil {
		return fmt.Errorf("error when marshaling pub/sub message: %w", err)
	}

	// Pub/Sub is best-effort. The event is already persisted in the stream,
	// so other instances catch up via history even if this publish fails.
	if err := t.client.Publish(ctx, t.channel, string(pubsubPayload)).Err(); err != nil {
		t.logger.LogAttrs(ctx, slog.LevelError, "Failed to publish to Redis Pub/Sub (event persisted in stream)",
			slog.Any("error", err))
	}

	t.mutex.Lock()
	t.lastEventID = update.ID
	t.mutex.Unlock()

	for _, subscriber := range t.subscribers.MatchAny(update) {
		subscriber.Dispatch(ctx, update, false)
	}

	return nil
}

func (t *RedisTransport) AddSubscriber(ctx context.Context, subscriber *mercure.LocalSubscriber) error {
	select {
	case <-t.closed:
		return mercure.ErrClosedTransport
	default:
	}

	t.subscribers.Add(subscriber)

	// Guards a race where Close() between the first select and Add() would miss this
	// subscriber during its disconnect walk, leaving a zombie SSE connection open forever.
	select {
	case <-t.closed:
		t.subscribers.Remove(subscriber)
		subscriber.Disconnect()
		return mercure.ErrClosedTransport
	default:
	}

	if subscriber.RequestLastEventID != "" {
		if err := t.dispatchHistory(ctx, subscriber); err != nil {
			return err
		}
	}

	subscriber.Ready(ctx)

	return nil
}

// currentLastEventID returns the latest known event ID from memory (no Redis round-trip).
func (t *RedisTransport) currentLastEventID() string {
	t.mutex.RLock()
	defer t.mutex.RUnlock()

	return t.lastEventID
}

// historyReplayCursor returns the exclusive stream cursor to replay from for a
// reconnecting subscriber's requested Last-Event-ID.
//
// replay is false when the ID can't be anchored (trimmed or unknown). Callers MUST NOT
// then scan the stream, because Last-Event-ID is client-controlled and an arbitrary ID
// would make each reconnect read the entire stream.
func (t *RedisTransport) historyReplayCursor(ctx context.Context, requestedID string) (cursor string, replay bool) {
	if requestedID == mercure.EarliestLastEventID {
		return "-", true
	}

	resolvedCursor, found := t.resolveStreamCursor(ctx, requestedID)
	if !found {
		return "", false
	}

	return resolvedCursor, true
}

// dispatchHistory replays missed events to a reconnecting SSE subscriber, starting
// just after its requested Last-Event-ID.
//
// Reads are capped at historyLimit entries (0 disables replay entirely). The stream
// is shared across all topics with no per-topic filtering, so an uncapped replay
// would read the whole stream on every reconnect. Exceeding the budget (or an
// unanchorable Last-Event-ID) fast-forwards the subscriber to live instead.
func (t *RedisTransport) dispatchHistory(ctx context.Context, subscriber *mercure.LocalSubscriber) error {
	if t.historyLimit == 0 {
		subscriber.HistoryDispatched(t.currentLastEventID())

		return nil
	}

	cursor, replay := t.historyReplayCursor(ctx, subscriber.RequestLastEventID)
	if !replay {
		t.logger.LogAttrs(ctx, slog.LevelWarn, "Requested LastEventID not found in stream index, skipping history replay",
			slog.String("requestedID", subscriber.RequestLastEventID))
		subscriber.HistoryDispatched(t.currentLastEventID())

		return nil
	}

	responseLastEventID := subscriber.RequestLastEventID
	var scanned int64

	for {
		pageSize := historyPageSize
		if remaining := t.historyLimit - scanned; remaining < pageSize {
			pageSize = remaining
		}

		messages, err := t.client.XRangeN(ctx, t.stream, cursor, "+", pageSize).Result()
		if err != nil {
			return fmt.Errorf("error reading Redis Stream history: %w", err)
		}

		if len(messages) == 0 {
			break
		}

		for _, message := range messages {
			data, ok := message.Values["data"].(string)
			if !ok {
				continue
			}

			var update mercure.Update
			if err := json.Unmarshal([]byte(data), &update); err != nil {
				t.logger.LogAttrs(ctx, slog.LevelError, "Unable to unmarshal update from Redis Stream", slog.Any("error", err))
				continue
			}

			responseLastEventID = update.ID
			if subscriber.Match(&update) && !subscriber.Dispatch(ctx, &update, true) {
				subscriber.HistoryDispatched(responseLastEventID)
				return nil
			}
		}

		scanned += int64(len(messages))

		if int64(len(messages)) < pageSize {
			break
		}

		if scanned >= t.historyLimit {
			t.logger.LogAttrs(ctx, slog.LevelWarn, "History replay limit reached, fast-forwarding subscriber",
				slog.Int64("limit", t.historyLimit),
				slog.String("requestedID", subscriber.RequestLastEventID))
			subscriber.HistoryDispatched(responseLastEventID)

			return nil
		}

		cursor = "(" + messages[len(messages)-1].ID
	}

	subscriber.HistoryDispatched(responseLastEventID)

	return nil
}

func (t *RedisTransport) RemoveSubscriber(_ context.Context, subscriber *mercure.LocalSubscriber) error {
	t.subscribers.Remove(subscriber)
	return nil
}

func (t *RedisTransport) GetSubscribers(_ context.Context) (string, []*mercure.Subscriber, error) {
	t.mutex.RLock()
	defer t.mutex.RUnlock()

	var subscribers []*mercure.Subscriber
	t.subscribers.Walk(0, func(subscriber *mercure.LocalSubscriber) bool {
		subscribers = append(subscribers, &subscriber.Subscriber)
		return true
	})

	return t.lastEventID, subscribers, nil
}

func (t *RedisTransport) Close(_ context.Context) (err error) {
	t.closedOnce.Do(func() {
		close(t.closed)
		t.cancel()

		t.subscribers.Walk(0, func(subscriber *mercure.LocalSubscriber) bool {
			subscriber.Disconnect()
			return true
		})

		err = t.client.Close()
	})

	if err != nil {
		return fmt.Errorf("unable to close Redis connection: %w", err)
	}

	return nil
}

var (
	_ mercure.Transport            = (*RedisTransport)(nil)
	_ mercure.TransportSubscribers = (*RedisTransport)(nil)
)
