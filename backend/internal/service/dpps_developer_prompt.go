package service

import (
	"github.com/Wei-Shaw/sub2api/internal/config"
)

// dppsPromoteDeveloperPrompt is DPPS's switchable fix for clients that put their own
// system prompt in `input` as a role:"developer" item and send no `instructions`
// (deepseek-harness via pi-ai's openai-responses provider). Without it the OAuth
// transform injects the ~21 KB Codex persona as `instructions` and the client's prompt
// is demoted below it.
//
// When gateway.dpps_developer_prompt_as_instructions is true (default), the request has no
// instructions, is not from the Codex CLI and has developer items, those items are
// relabelled role:"system" so extractSystemMessagesFromInput promotes them exactly like
// system items and applyInstructions finds instructions already set. Anything else is
// left untouched. Reports whether the body was changed.
func dppsPromoteDeveloperPrompt(reqBody map[string]any, cfg *config.Config, isCodexCLI bool) bool {
	if cfg == nil || !cfg.Gateway.DppsDeveloperPromptAsInstructions || isCodexCLI {
		return false
	}
	if !isInstructionsEmpty(reqBody) {
		return false
	}
	input, ok := reqBody["input"].([]any)
	if !ok {
		return false
	}
	changed := false
	for _, item := range input {
		if m, ok := item.(map[string]any); ok && m["role"] == "developer" {
			m["role"] = "system"
			changed = true
		}
	}
	return changed
}
