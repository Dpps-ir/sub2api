package handler

import (
	"testing"

	"github.com/Wei-Shaw/sub2api/internal/pkg/claude"
	"github.com/Wei-Shaw/sub2api/internal/pkg/xai"
	"github.com/Wei-Shaw/sub2api/internal/service"
	"github.com/stretchr/testify/require"
)

// The events webhook hashes what /v1/models serves, including the platform
// default lists a group falls back to when no account maps any model.
func TestDppsServedModelIDsFromAvailable_UsesTheSameFallbackAsModelsEndpoint(t *testing.T) {
	anthropic := &service.Group{ID: 1, Platform: service.PlatformAnthropic}
	want := make([]string, 0, len(claude.DefaultModels))
	for _, m := range claude.DefaultModels {
		want = append(want, m.ID)
	}
	require.Equal(t, want, dppsServedModelIDsFromAvailable(anthropic, nil), "no mapping: the default claude list, not an empty one")

	require.Equal(t, []string{"claude-x"}, dppsServedModelIDsFromAvailable(anthropic, []string{"claude-x"}), "explicit mapping wins")
	require.Equal(t, xai.DefaultModelIDs(), dppsServedModelIDsFromAvailable(&service.Group{ID: 2, Platform: service.PlatformGrok}, nil))
	require.NotEmpty(t, dppsServedModelIDsFromAvailable(&service.Group{ID: 3, Platform: service.PlatformOpenAI}, nil), "openai default list")
	require.Equal(t, []string{}, dppsServedModelIDsFromAvailable(&service.Group{ID: 4, Platform: service.PlatformOpenAI}, []string{}),
		"an openai group whose accounts map nothing serves an empty list, as /v1/models does")
	require.NotEmpty(t, dppsServedModelIDsFromAvailable(&service.Group{ID: 5, Platform: service.PlatformGemini}, nil))
}
