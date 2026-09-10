package webrtc

import (
	"context"
	"encoding/json"
	"errors"
	"sync/atomic"
	"testing"
	"time"

	pion "github.com/pion/webrtc/v4"
)

func chunkTestManager(id string, store func(string, ChunkStoreRequest) error) *Manager {
	return NewManager(id, make(chan ChatMessage, 1), store, nil, nil, nil, nil, nil, nil, nil, nil, nil, nil, nil, nil)
}

func connectedChunkManagers(t *testing.T, store func(string, ChunkStoreRequest) error) (*Manager, *Manager) {
	t.Helper()
	a, b := chunkTestManager("a", nil), chunkTestManager("b", store)
	t.Cleanup(func() { a.CloseAll(); b.CloseAll() })
	offer, err := a.CreateOffer("b")
	if err != nil {
		t.Fatal(err)
	}
	answer, err := b.HandleOffer("a", offer)
	if err != nil {
		t.Fatal(err)
	}
	if err := a.SetRemoteDescription("b", answer); err != nil {
		t.Fatal(err)
	}
	deadline := time.Now().Add(5 * time.Second)
	for !a.IsConnected("b") || !b.IsConnected("a") {
		if time.Now().After(deadline) {
			t.Fatal("local data channels did not open")
		}
		time.Sleep(10 * time.Millisecond)
	}
	return a, b
}

func TestChunkStorageAcknowledgementFollowsSuccessfulWrite(t *testing.T) {
	var fail atomic.Bool
	fail.Store(true)
	var saved atomic.Int32
	a, _ := connectedChunkManagers(t, func(_ string, req ChunkStoreRequest) error {
		if fail.Load() {
			return errors.New("disk full")
		}
		saved.Add(1)
		return nil
	})
	batch := ChunkStoreBatchRequest{Chunks: []ChunkStoreRequest{{FileID: "file", ChunkIndex: 0, Data: []byte("payload")}}}
	if err := a.SendChunkStoreBatchConfirmed(context.Background(), "b", batch); err == nil {
		t.Fatal("failed write was acknowledged as stored")
	}
	fail.Store(false)
	if err := a.SendChunkStoreBatchConfirmed(context.Background(), "b", batch); err != nil {
		t.Fatal(err)
	}
	if saved.Load() != 1 {
		t.Fatal("ACK arrived before the payload was stored")
	}
	a.chunkAckMu.Lock()
	remaining := len(a.chunkAcks)
	a.chunkAckMu.Unlock()
	if remaining != 0 {
		t.Fatal("acknowledgement waiter leaked")
	}
}

func TestLegacyStorageWithoutAckStaysUnconfirmed(t *testing.T) {
	var saved atomic.Int32
	a, b := connectedChunkManagers(t, func(_ string, _ ChunkStoreRequest) error { saved.Add(1); return nil })
	// An old peer ignores the additive request_id field and sends no ACK.
	b.mu.RLock()
	dc := b.dataChans["a"]
	b.mu.RUnlock()
	dc.OnMessage(func(message pion.DataChannelMessage) {
		var env envelope
		if json.Unmarshal(message.Data, &env) != nil {
			return
		}
		if env.Type == MsgTypeChunkStoreBatch {
			var batch ChunkStoreBatchRequest
			if json.Unmarshal(env.Data, &batch) != nil {
				return
			}
			batch.RequestID = ""
			env.Data, _ = json.Marshal(batch)
			message.Data, _ = json.Marshal(env)
		}
		b.onMessage("a", message)
	})
	ctx, cancel := context.WithTimeout(context.Background(), 200*time.Millisecond)
	defer cancel()
	err := a.SendChunkStoreBatchConfirmed(ctx, "b", ChunkStoreBatchRequest{Chunks: []ChunkStoreRequest{{FileID: "file", Data: []byte("payload")}}})
	if !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("missing ACK error=%v", err)
	}
	if saved.Load() != 1 {
		t.Fatal("legacy storage must still receive the compatible payload")
	}
}

func TestChunkStorageAckMustMatchRequestAndPeer(t *testing.T) {
	m := chunkTestManager("a", nil)
	result := make(chan ChunkStoreAck, 1)
	m.chunkAcks["request"] = chunkStoreWaiter{peerID: "b", result: result}
	m.acceptChunkStoreAck("other", ChunkStoreAck{RequestID: "request"})
	m.acceptChunkStoreAck("b", ChunkStoreAck{RequestID: "different"})
	if len(result) != 0 {
		t.Fatal("unrelated acknowledgement completed a transfer")
	}
	m.acceptChunkStoreAck("b", ChunkStoreAck{RequestID: "request"})
	if len(result) != 1 {
		t.Fatal("matching acknowledgement was ignored")
	}
}
