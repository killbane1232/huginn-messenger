package messenger

import (
	"fmt"

	"github.com/killbane1232/huginn-messenger/internal/config"
	"github.com/killbane1232/huginn-messenger/internal/muninn"
)

func normalizeMuninnICE(profiles map[string]config.ICEConfig, addresses []string) (map[string]config.ICEConfig, error) {
	allowed := make(map[string]bool, len(addresses))
	for _, address := range addresses {
		allowed[address] = true
	}
	result := map[string]config.ICEConfig{}
	for raw, profile := range profiles {
		origins, err := muninn.NormalizeServers([]string{raw})
		if err != nil {
			return nil, fmt.Errorf("invalid Muninn origin in ICE settings")
		}
		address := origins[0]
		if !allowed[address] {
			continue // Removing a Muninn also removes its local credentials.
		}
		if _, exists := result[address]; exists {
			return nil, fmt.Errorf("duplicate Muninn ICE profile")
		}
		profile, err = profile.Normalized()
		if err != nil {
			return nil, err
		}
		result[address] = profile
	}
	return result, nil
}

func (m *Messenger) applyMuninnICE(cfg *config.Config, address string) {
	if m.rtcManager != nil && !m.fixedICEServers {
		m.rtcManager.SetICEServers(cfg.MuninnICE[address].Servers())
	}
}
