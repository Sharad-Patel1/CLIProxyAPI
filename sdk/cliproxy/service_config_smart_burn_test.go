package cliproxy

import (
	"os"
	"path/filepath"
	"testing"

	internalconfig "github.com/router-for-me/CLIProxyAPI/v8/internal/config"
	coreauth "github.com/router-for-me/CLIProxyAPI/v8/sdk/cliproxy/auth"
)

func TestSmartBurnRoutingConfig(t *testing.T) {
	for _, affinity := range []bool{false, true} {
		path := filepath.Join(t.TempDir(), "config.yaml")
		if err := os.WriteFile(path, []byte("routing:\n  strategy: smart-burn\n"), 0600); err != nil {
			t.Fatal(err)
		}
		cfg, err := internalconfig.LoadConfig(path)
		if err != nil {
			t.Fatal(err)
		}
		cfg.Routing.SessionAffinity = affinity
		state := normalizedRoutingRuntimeState(cfg)
		if state.strategy != "smart-burn" {
			t.Fatalf("strategy = %q", state.strategy)
		}
		selector := newRoutingSelector(state)
		if affinity {
			sticky, ok := selector.(*coreauth.SessionAffinitySelector)
			if !ok {
				t.Fatalf("selector = %T", selector)
			}
			sticky.Stop()
		} else if _, ok := selector.(*coreauth.SmartBurnSelector); !ok {
			t.Fatalf("selector = %T", selector)
		}
	}
}
