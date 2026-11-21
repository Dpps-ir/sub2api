package handler

import (
	"context"

	"github.com/Wei-Shaw/sub2api/internal/pkg/claude"
	"github.com/Wei-Shaw/sub2api/internal/pkg/geminicli"
	"github.com/Wei-Shaw/sub2api/internal/pkg/openai"
	"github.com/Wei-Shaw/sub2api/internal/pkg/typesafe"
	"github.com/Wei-Shaw/sub2api/internal/pkg/xai"
	"github.com/Wei-Shaw/sub2api/internal/service"
)

// servedModelIDs is the list GET /v1/models serves to a key of this group: the
// same decisions as GatewayHandler.Models (platform fallback lists, allowlist,
// composite routing), returned as ids instead of written as a response. The
// events webhook hashes it, so a change is a change a client would see.
// A pinned OpenAI Codex manifest is not followed (it lists live upstream models).
func (h *GatewayHandler) servedModelIDs(ctx context.Context, group *service.Group) []string {
	if h == nil || h.gatewayService == nil || group == nil {
		return nil
	}
	groupID := &group.ID
	platform := group.Platform

	if platform == service.PlatformComposite {
		available := h.compositeAvailableModels(ctx, groupID, "", true)
		if group.ModelAllowlistEnabled() {
			source := available
			if len(source) == 0 {
				source = defaultModelIDsForPlatform(service.PlatformComposite)
			}
			return group.ModelAllowlist.FilterForListing(source)
		}
		if len(available) > 0 {
			return available
		}
		return defaultModelIDsForPlatform(service.PlatformComposite)
	}

	return servedModelIDsFromAvailable(group, h.gatewayService.GetAvailableModels(ctx, groupID, platform))
}

// servedModelIDsFromAvailable applies the allowlist and the platform fallback
// lists to the account-derived list, exactly as GatewayHandler.Models does.
func servedModelIDsFromAvailable(group *service.Group, available []string) []string {
	platform := group.Platform
	if group.ModelAllowlistEnabled() {
		return group.ModelAllowlist.FilterForListing(modelListingSource(platform, available, defaultModelIDsForPlatform(platform)))
	}
	if len(available) > 0 || (platform == service.PlatformOpenAI && available != nil) {
		return available
	}
	switch platform {
	case service.PlatformOpenAI:
		ids := make([]string, 0, len(openai.DefaultModels))
		for _, m := range openai.DefaultModels {
			ids = append(ids, m.ID)
		}
		return ids
	case service.PlatformGemini:
		ids := make([]string, 0, len(geminicli.DefaultModels))
		for _, m := range geminicli.DefaultModels {
			ids = append(ids, m.ID)
		}
		return ids
	case service.PlatformGrok:
		return xai.DefaultModelIDs()
	case service.PlatformTypeSafe:
		return []string{typesafe.JevLatestModel}
	}
	ids := make([]string, 0, len(claude.DefaultModels))
	for _, m := range claude.DefaultModels {
		ids = append(ids, m.ID)
	}
	return ids
}
