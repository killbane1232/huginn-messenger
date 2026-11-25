package config

import (
	"fmt"
	"strings"

	"github.com/pion/stun/v3"
	pion "github.com/pion/webrtc/v4"
)

// ICEConfig belongs to one Muninn origin. Credentials are local settings and
// must never be included in discovery hints or registration requests.
type ICEConfig struct {
	STUNURLs     []string `json:"stun_urls"`
	TURNURLs     []string `json:"turn_urls"`
	TURNUsername string   `json:"turn_username"`
	TURNPassword string   `json:"turn_password"`
}

func (c ICEConfig) Normalized() (ICEConfig, error) {
	normalize := func(addresses []string, relay bool) ([]string, error) {
		result := []string{}
		seen := map[string]bool{}
		for _, raw := range addresses {
			address := strings.TrimSpace(raw)
			if _, query, hasQuery := strings.Cut(address, "?"); hasQuery && (!relay || query != "transport=udp" && query != "transport=tcp") {
				return nil, fmt.Errorf("invalid STUN/TURN transport")
			}
			u, err := stun.ParseURI(address)
			if err != nil || strings.ContainsAny(address, "/@# \t\r\n") || u.Port < 1 || u.Port > 65535 {
				return nil, fmt.Errorf("invalid STUN/TURN URL")
			}
			isTURN := u.Scheme == stun.SchemeTypeTURN || u.Scheme == stun.SchemeTypeTURNS
			if isTURN != relay {
				return nil, fmt.Errorf("STUN and TURN URLs must use their respective schemes")
			}
			if !seen[address] {
				result = append(result, address)
				seen[address] = true
			}
		}
		return result, nil
	}
	var err error
	if c.STUNURLs, err = normalize(c.STUNURLs, false); err != nil {
		return ICEConfig{}, err
	}
	if c.TURNURLs, err = normalize(c.TURNURLs, true); err != nil {
		return ICEConfig{}, err
	}
	c.TURNUsername = strings.TrimSpace(c.TURNUsername)
	if len(c.TURNURLs) > 0 && (c.TURNUsername == "" || c.TURNPassword == "") {
		return ICEConfig{}, fmt.Errorf("TURN URLs require a username and password")
	}
	if len(c.TURNURLs) == 0 {
		c.TURNUsername = ""
		c.TURNPassword = ""
	}
	return c, nil
}

func (c ICEConfig) Servers() []pion.ICEServer {
	servers := []pion.ICEServer{}
	if len(c.STUNURLs) > 0 {
		servers = append(servers, pion.ICEServer{URLs: append([]string(nil), c.STUNURLs...)})
	}
	if len(c.TURNURLs) > 0 {
		servers = append(servers, pion.ICEServer{
			URLs: append([]string(nil), c.TURNURLs...), Username: c.TURNUsername,
			Credential: c.TURNPassword,
		})
	}
	return servers
}

func CloneMuninnICE(profiles map[string]ICEConfig) map[string]ICEConfig {
	if profiles == nil {
		return nil
	}
	result := make(map[string]ICEConfig, len(profiles))
	for address, profile := range profiles {
		profile.STUNURLs = append([]string(nil), profile.STUNURLs...)
		profile.TURNURLs = append([]string(nil), profile.TURNURLs...)
		result[address] = profile
	}
	return result
}
