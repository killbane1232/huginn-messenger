package main

import (
	"testing"

	"github.com/killbane1232/huginn-messenger/internal/muninn"
)

func TestParsePeerFlag(t *testing.T) {
	tests := []struct {
		name  string
		value string
		want  muninn.PeerFlag
		ok    bool
	}{
		{name: "legacy default", want: muninn.PeerFlagThin, ok: true},
		{name: "thin", value: "thin", want: muninn.PeerFlagThin, ok: true},
		{name: "thick", value: "thick", want: muninn.PeerFlagThick, ok: true},
		{name: "very thick", value: "very_thick", want: muninn.PeerFlagVeryThick, ok: true},
		{name: "invalid", value: "storage", ok: false},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			got, ok := parsePeerFlag(test.value)
			if got != test.want || ok != test.ok {
				t.Fatalf("parsePeerFlag(%q) = (%q, %v), want (%q, %v)", test.value, got, ok, test.want, test.ok)
			}
		})
	}
}
