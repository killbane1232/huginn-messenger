package config

import "testing"

func TestICEProfileValidation(t *testing.T) {
	for _, profile := range []ICEConfig{
		{STUNURLs: []string{"https://host"}},
		{STUNURLs: []string{"turn:host:3478"}},
		{TURNURLs: []string{"stun:host:3478"}, TURNUsername: "u", TURNPassword: "p"},
		{STUNURLs: []string{"stun:host:0"}},
		{STUNURLs: []string{"stun:host:65536"}},
		{STUNURLs: []string{"stun:host:3478#fragment"}},
		{STUNURLs: []string{"stun:user@host:3478"}},
		{TURNURLs: []string{"turn:host:3478"}},
	} {
		if _, err := profile.Normalized(); err == nil {
			t.Fatal("accepted invalid ICE profile")
		}
	}
	profile, err := (ICEConfig{STUNURLs: []string{" stun:host:3478 ", "stun:host:3478"}, TURNURLs: []string{"turns:[::1]:5349?transport=tcp"}, TURNUsername: " user ", TURNPassword: " password "}).Normalized()
	if err != nil || len(profile.STUNURLs) != 1 || profile.TURNUsername != "user" || profile.TURNPassword != " password " {
		t.Fatal("normalization lost or modified credentials")
	}
	cleared, err := (ICEConfig{TURNUsername: "old", TURNPassword: "old"}).Normalized()
	if err != nil || cleared.TURNUsername != "" || cleared.TURNPassword != "" {
		t.Fatal("clearing relay URLs did not clear credentials")
	}
}
