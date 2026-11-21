//go:build unit

package config

import (
	"testing"

	"github.com/stretchr/testify/require"
)

func TestDppsDeveloperPromptAsInstructions_DefaultOnEnvOff(t *testing.T) {
	for _, tc := range []struct {
		name string
		env  string
		want bool
	}{{"default", "", true}, {"env_false", "false", false}} {
		t.Run(tc.name, func(t *testing.T) {
			resetViperWithJWTSecret(t)
			t.Chdir(t.TempDir())
			t.Setenv("GATEWAY_DPPS_DEVELOPER_PROMPT_AS_INSTRUCTIONS", tc.env)
			cfg, err := Load()
			require.NoError(t, err)
			require.Equal(t, tc.want, cfg.Gateway.DppsDeveloperPromptAsInstructions)
		})
	}
}
