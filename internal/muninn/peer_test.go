package muninn

import "testing"

func TestPeerIdentityNormalizesEmbeddedSignature(t *testing.T) {
	tests := []struct {
		name        string
		peer        Peer
		wantLogin   string
		wantPeerKey string
	}{
		{
			name:        "separate login and signature",
			peer:        Peer{Login: "alice", SignatureKey: "signature"},
			wantLogin:   "alice",
			wantPeerKey: "alice:signature",
		},
		{
			name:        "signature already embedded in login",
			peer:        Peer{Login: "alice:signature", SignatureKey: "signature"},
			wantLogin:   "alice",
			wantPeerKey: "alice:signature",
		},
		{
			name: "group uses id for routing and name for display",
			peer: Peer{
				ID:           "group-id",
				Login:        "Family",
				SignatureKey: "signature",
				IsFake:       true,
			},
			wantLogin:   "Family",
			wantPeerKey: "group-id:signature",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := tt.peer.DisplayLogin(); got != tt.wantLogin {
				t.Fatalf("DisplayLogin() = %q, want %q", got, tt.wantLogin)
			}
			if got := tt.peer.Key(); got != tt.wantPeerKey {
				t.Fatalf("Key() = %q, want %q", got, tt.wantPeerKey)
			}
		})
	}
}
