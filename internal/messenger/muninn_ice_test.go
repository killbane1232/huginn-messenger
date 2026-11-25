package messenger

import (
	"path/filepath"
	"reflect"
	"testing"
	"time"

	"github.com/killbane1232/huginn-messenger/internal/config"
	"github.com/killbane1232/huginn-messenger/internal/muninn"
	pion "github.com/pion/webrtc/v4"
)

func TestICEProfilesFollowSelectedMuninn(t *testing.T) {
	a := newReconnectServer(t, 40*time.Millisecond)
	b := newReconnectServer(t, time.Millisecond)
	profileA := config.ICEConfig{STUNURLs: []string{"stun:a.example:3478"}, TURNURLs: []string{"turn:a.example:3478?transport=udp"}, TURNUsername: "alice", TURNPassword: "secret-a"}
	profileB := config.ICEConfig{STUNURLs: []string{"stun:b.example:3478"}, TURNURLs: []string{"turns:b.example:5349?transport=tcp"}, TURNUsername: "bob", TURNPassword: "secret-b"}
	m, err := New("alice", muninn.NewClient(a.server.URL), filepath.Join(t.TempDir(), "huginn.db"),
		WithMuninnServers([]string{b.server.URL}),
		WithMuninnICE(map[string]config.ICEConfig{a.server.URL: profileA, b.server.URL: profileB}))
	if err != nil {
		t.Fatal(err)
	}
	defer m.Shutdown()
	if m.Config().MuninnAddr != b.server.URL {
		t.Fatal("startup did not select B")
	}
	connection := func(id string, want []pion.ICEServer) *pion.PeerConnection {
		t.Helper()
		pc, err := m.rtcManager.NewPeerConnection(id)
		if err != nil {
			t.Fatal(err)
		}
		if got := pc.GetConfiguration().ICEServers; !reflect.DeepEqual(got, want) {
			t.Fatalf("wrong ICE profile for %s", id)
		}
		return pc
	}
	before := connection("before-switch", profileB.Servers())
	copy := m.Config()
	copy.MuninnICE[b.server.URL].TURNURLs[0] = "turn:changed.example:3478"
	delete(copy.MuninnICE, a.server.URL)
	if !reflect.DeepEqual(m.Config().MuninnICE[b.server.URL].Servers(), profileB.Servers()) || len(m.Config().MuninnICE) != 2 {
		t.Fatal("config returned mutable ICE aliases")
	}
	waitForMuninn(t, time.Second, func() bool { return m.wsClient.IsConnected() && b.connection.Load() != nil })
	a.delay.Store(int64(time.Millisecond))
	b.wsFailed.Store(true)
	b.connection.Load().Close()
	waitForMuninn(t, 8*time.Second, func() bool { return m.Config().MuninnAddr == a.server.URL && m.wsClient.IsConnected() })
	connection("after-switch", profileA.Servers())
	if !reflect.DeepEqual(before.GetConfiguration().ICEServers, profileB.Servers()) {
		t.Fatal("switch rewrote an existing peer connection")
	}
	stored, err := m.store.LoadAppConfig()
	if err != nil || !reflect.DeepEqual(stored.MuninnICE, m.Config().MuninnICE) {
		t.Fatal("selection lost a saved profile")
	}

	cfg := m.Config()
	cfg.MuninnICE[a.server.URL] = config.ICEConfig{}
	if err := m.SaveConfig(cfg); err != nil {
		t.Fatal(err)
	}
	connection("disabled-ice", []pion.ICEServer{})
	cfg = m.Config()
	cfg.MuninnServers = nil
	if err := m.SaveConfig(cfg); err != nil {
		t.Fatal(err)
	}
	stored, err = m.store.LoadAppConfig()
	if err != nil || len(stored.MuninnICE) != 1 {
		t.Fatal("removed Muninn retained its credentials")
	}
}

func TestInvalidICEProfileIsNotSaved(t *testing.T) {
	srv := newReconnectServer(t, time.Millisecond)
	m, err := New("alice", muninn.NewClient(srv.server.URL), filepath.Join(t.TempDir(), "huginn.db"))
	if err != nil {
		t.Fatal(err)
	}
	defer m.Shutdown()
	cfg := m.Config()
	cfg.MuninnICE = map[string]config.ICEConfig{srv.server.URL: {TURNURLs: []string{"turn:relay.example:3478"}}}
	if err := m.SaveConfig(cfg); err == nil {
		t.Fatal("accepted TURN without credentials")
	}
	stored, err := m.store.LoadAppConfig()
	if err != nil || len(stored.MuninnICE) != 0 {
		t.Fatal("invalid profile changed persisted settings")
	}
	pc, err := m.rtcManager.NewPeerConnection("no-default-relay")
	if err != nil || len(pc.GetConfiguration().ICEServers) != 0 {
		t.Fatal("unconfigured Muninn used a global relay")
	}
}
