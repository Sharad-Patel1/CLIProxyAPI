# Custom fleet gateway

Based on upstream v8.0.55. The `feat/smart-burn-gateway` branch adds `routing.strategy: smart-burn`.

New sessions select an eligible account with the earliest future weekly reset. Claude uses its main 7-day watermark; Codex identifies the weekly window by its 10,080-minute duration, whether it appears as primary or secondary. An account with no usable weekly reset falls back to its short-window reset. Relative Codex reset values are anchored to the observation timestamp. Expired and malformed dates are ignored. Named model and code-review quota buckets do not affect unrelated traffic.

Existing credential priority, model eligibility, cooldown, retry exclusion, and WebSocket capability rules still apply. Equal reset dates use credential ID as a stable tie-breaker. A supported account with missing/expired data receives one ordinary new-session discovery selection per five minutes. This also bounds discovery while the first request is still in flight. Known dates win between discovery selections; pools without usable quota dates fall back to round-robin. There is no background upstream polling.

Use the existing session-affinity wrapper:

```yaml
routing:
  strategy: smart-burn
  session-affinity: true
  session-affinity-ttl: 24h
  session-affinity-subagents: true
  session-affinity-rebind-on: quota
```

The TTL is refreshed by activity. Bindings are retained across changing quota rankings and temporary upstream failures. Quota, authentication, disabled credentials, and unavailable models can cause failover. Bindings remain scoped by provider, model, and client session identity, following upstream behavior. A service restart can discard in-memory affinity.

Codex needs both sides configured: `supports_websockets = true` in the client provider and `"websockets": true` in each hub Codex OAuth credential. Keep `ws-auth: true` on legacy hub configs. No latency percentage is claimed; verify actual upstream WebSocket use before measuring performance.

## Validation

```sh
gofmt -l sdk/cliproxy/auth/selector_smart_burn*.go sdk/cliproxy/service_config_smart_burn_test.go
go test ./sdk/cliproxy/auth ./sdk/cliproxy
go test -race ./sdk/cliproxy/auth -run 'TestSmartBurn|TestManagerSessionAffinityAliasCooldownPreservesSelection|TestManagerAliasQuotaFailoverWithUnobservedTargetModel'
go test ./...
go vet ./...
go build -trimpath -o bin/cliproxyapi ./cmd/server
```

On 2026-11-05, the full suite and vet each reproduced one unrelated failure on clean upstream v8.0.55:

- `internal/util/TestInPlaceByteWritesAreReviewed` lacks a review entry for the existing copy in `internal/kvstore/bolt_store.go`.
- `internal/pluginhost/host_test.go:1637` copies a `sync.Once` value.

The routing tests, race checks, and build pass. The management panel must recognize `smart-burn` in both routing and visual-config normalization, otherwise saving settings can restore round-robin.

The controller's `~/fleet` owns client rollout through `fleet gateway apply [machine]` and `fleet gateway check [machine]`. The gateway client key stays in private fleet secrets; upstream OAuth credentials stay on the gateway. Per-user service paths, backups, rollout evidence, and rollback instructions live in `~/.config/cliproxyapi/SETUP.md` on the gateway.
