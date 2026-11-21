//go:build unit

package service

import (
	"encoding/json"
	"strings"
	"testing"

	"github.com/Wei-Shaw/sub2api/internal/config"
	"github.com/stretchr/testify/require"
)

// dppsAskRequest builds a Responses body shaped like deepseek-harness 0.2.2-rc.5 sends
// through @earendil-works/pi-ai's openai-responses provider: the system prompt is an
// input item with role "developer" (reasoning models) or "system", `instructions` is
// absent, tools are plain non-strict function tools, no tool_choice.
func dppsAskRequest(t *testing.T, promptRole string) map[string]any {
	t.Helper()
	raw := `{
	  "model":"gpt-5.5","stream":true,"store":false,"prompt_cache_key":"sess-1",
	  "reasoning":{"effort":"medium","summary":"auto"},"include":["reasoning.encrypted_content"],
	  "input":[
	    {"role":"` + promptRole + `","content":"You are an AI agent powered by DeepSeek Harness. Use ask_user_question when you need a decision."},
	    {"role":"user","content":[{"type":"input_text","text":"build me a site"}]}
	  ],
	  "tools":[
	    {"type":"function","name":"read","description":"read a file","parameters":{"type":"object","properties":{"path":{"type":"string"}},"required":["path"]},"strict":false},
	    {"type":"function","name":"ask_user_question","description":"Ask the user a concise question when you need confirmation, a choice, or missing information before proceeding.",
	     "parameters":{"type":"object","properties":{"questions":{"type":"array","items":{"type":"object","additionalProperties":true,"properties":{"id":{"type":"string"},"question":{"type":"string"}}}}},"required":["questions"]},"strict":false}
	  ]}`
	var m map[string]any
	require.NoError(t, json.Unmarshal([]byte(raw), &m))
	return m
}

func dppsToolNames(m map[string]any) []string {
	var out []string
	for _, tl := range m["tools"].([]any) {
		out = append(out, tl.(map[string]any)["name"].(string))
	}
	return out
}

func TestDppsAskTool_OAuthTransform_BaselineUnchanged(t *testing.T) {
	for _, role := range []string{"developer", "system"} {
		t.Run(role, func(t *testing.T) {
			req := dppsAskRequest(t, role)
			before, _ := json.Marshal(req["tools"])
			res := applyCodexOAuthTransformWithOptions(req, codexOAuthTransformOptions{OmitPromotedSystemMessagesFromInput: true})
			require.NoError(t, res.Error)
			after, _ := json.Marshal(req["tools"])
			require.JSONEq(t, string(before), string(after), "tools must reach upstream unchanged")
			require.Contains(t, dppsToolNames(req), "ask_user_question")
			require.Nil(t, res.ToolNameReverse)
			instr, _ := req["instructions"].(string)
			inp, _ := json.Marshal(req["input"])
			t.Logf("role=%s tools=%v\ninstructions(first 120)=%q (len %d)\nclient prompt in instructions=%v, in input=%v",
				role, dppsToolNames(req), dppsFirstN(instr, 120), len(instr),
				strings.Contains(instr, "ask_user_question"), strings.Contains(string(inp), "ask_user_question"))
			t.Logf("tools out=%s", after)
			if role == "system" {
				require.Contains(t, instr, "ask_user_question")
			} else {
				// developer role: client prompt stays in input; instructions = Codex base prompt
				require.NotContains(t, instr, "DeepSeek Harness")
				require.Contains(t, string(inp), "DeepSeek Harness")
			}
		})
	}
}

func dppsFirstN(s string, n int) string {
	if len(s) > n {
		return s[:n]
	}
	return s
}

func dppsCfg(on bool) *config.Config {
	c := &config.Config{}
	c.Gateway.DppsDeveloperPromptAsInstructions = on
	return c
}

// dppsRunOAuth mirrors the non-bridge OAuth call site in openai_gateway_forward.go:
// the DPPS hook, then the transform with the same options.
func dppsRunOAuth(t *testing.T, req map[string]any, cfg *config.Config, isCodexCLI bool) []byte {
	t.Helper()
	dppsPromoteDeveloperPrompt(req, cfg, isCodexCLI)
	res := applyCodexOAuthTransformWithOptions(req, codexOAuthTransformOptions{
		IsCodexCLI: isCodexCLI, OmitPromotedSystemMessagesFromInput: true,
	})
	require.NoError(t, res.Error)
	out, err := json.Marshal(req)
	require.NoError(t, err)
	return out
}

func TestDppsDeveloperPrompt_SwitchOff_Unchanged(t *testing.T) {
	for _, cfg := range []*config.Config{nil, dppsCfg(false)} {
		ref := dppsRunOAuth(t, dppsAskRequest(t, "developer"), nil, false)
		got := dppsRunOAuth(t, dppsAskRequest(t, "developer"), cfg, false)
		require.Equal(t, string(ref), string(got))
		var m map[string]any
		require.NoError(t, json.Unmarshal(got, &m))
		instr, _ := m["instructions"].(string)
		require.NotContains(t, instr, "DeepSeek Harness")
		require.Contains(t, instr, "Codex", "persona still injected with the switch off")
		require.Contains(t, string(got), "DeepSeek Harness", "client prompt stays a developer input item")
	}
}

func TestDppsDeveloperPrompt_SwitchOn_ClientPromptBecomesInstructions(t *testing.T) {
	persona := defaultCodexSynthInstructions("gpt-5.5")
	req := dppsAskRequest(t, "developer")
	toolsBefore, _ := json.Marshal(req["tools"])
	got := dppsRunOAuth(t, req, dppsCfg(true), false)
	var m map[string]any
	require.NoError(t, json.Unmarshal(got, &m))
	require.Equal(t, "You are an AI agent powered by DeepSeek Harness. Use ask_user_question when you need a decision.", m["instructions"])
	require.NotContains(t, string(got), persona[:200])
	require.Len(t, m["input"], 1, "developer item moved out of input; user item remains")
	toolsAfter, _ := json.Marshal(m["tools"])
	require.Equal(t, string(toolsBefore), string(toolsAfter), "tools byte-for-byte equal")

	// Same bytes as a client that sent the prompt as role "system" (the proven-working shape).
	sys := dppsRunOAuth(t, dppsAskRequest(t, "system"), dppsCfg(false), false)
	require.Equal(t, string(sys), string(got))
}

func TestDppsDeveloperPrompt_SwitchOn_OtherRequestsUnchanged(t *testing.T) {
	cfgOn := dppsCfg(true)
	t.Run("codex cli", func(t *testing.T) {
		ref := dppsRunOAuth(t, dppsAskRequest(t, "developer"), nil, true)
		got := dppsRunOAuth(t, dppsAskRequest(t, "developer"), cfgOn, true)
		require.Equal(t, string(ref), string(got))
	})
	t.Run("already has instructions", func(t *testing.T) {
		mk := func() map[string]any {
			r := dppsAskRequest(t, "developer")
			r["instructions"] = "client instructions"
			return r
		}
		require.Equal(t, string(dppsRunOAuth(t, mk(), nil, false)), string(dppsRunOAuth(t, mk(), cfgOn, false)))
	})
	t.Run("no developer item", func(t *testing.T) {
		mk := func() map[string]any {
			r := dppsAskRequest(t, "user")
			return r
		}
		require.Equal(t, string(dppsRunOAuth(t, mk(), nil, false)), string(dppsRunOAuth(t, mk(), cfgOn, false)))
	})
	t.Run("helper reports no change", func(t *testing.T) {
		r := dppsAskRequest(t, "user")
		require.False(t, dppsPromoteDeveloperPrompt(r, cfgOn, false))
		r = dppsAskRequest(t, "developer")
		require.False(t, dppsPromoteDeveloperPrompt(r, cfgOn, true))
		require.True(t, dppsPromoteDeveloperPrompt(r, cfgOn, false))
	})
}
