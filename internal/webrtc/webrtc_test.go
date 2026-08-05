package webrtc

import (
	"testing"
	"time"
)

func TestCloseDoesNotHoldManagerLockWhileClosingPeer(t *testing.T) {
	m := NewManager("local", make(chan ChatMessage, 1), nil, nil, nil, nil, nil, nil)
	if _, err := m.NewPeerConnection("remote"); err != nil {
		t.Fatal(err)
	}

	done := make(chan struct{})
	go func() {
		m.Close("remote")
		close(done)
	}()

	select {
	case <-done:
	case <-time.After(2 * time.Second):
		t.Fatal("Close blocked while the connection-state callback acquired the manager lock")
	}
}

func TestCloseAllDoesNotHoldManagerLockWhileClosingPeers(t *testing.T) {
	m := NewManager("local", make(chan ChatMessage, 1), nil, nil, nil, nil, nil, nil)
	for _, remoteID := range []string{"first", "second"} {
		if _, err := m.NewPeerConnection(remoteID); err != nil {
			t.Fatal(err)
		}
	}

	done := make(chan struct{})
	go func() {
		m.CloseAll()
		close(done)
	}()

	select {
	case <-done:
	case <-time.After(2 * time.Second):
		t.Fatal("CloseAll blocked while connection-state callbacks acquired the manager lock")
	}
}
