# Changelog

All notable changes to this project are documented here.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.1.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [0.1.1] — 2026-04-26

### Added

- **`Mycel.current_session` / `Mycel.current_session_id`** — module-level accessors that return the calling peer's `Channel::Session` (and its ID) while an RPC handler is executing. Backed by `Thread.current`, so handlers retain their original `register_method(name) { |*params| ... }` signatures *and* helper methods called from any depth can ask "who called me?" without threading a session_id through every call. Returns `nil` outside a handler context. Concurrent inbound calls land on independent worker threads and do not bleed.
- **`Mycel.with_current_session(session) { ... }`** — public wrapper used internally by `Peer#handle_method_request` to set the context. Also useful in tests to invoke a registered handler with a fake session attached.
- **`Channel::Session#session_id`** — accessor for the Hub-assigned identifier (set after Hub registration; `nil` before). Required for `Mycel.current_session_id` to work, and also handy for logging.
- **`Channel::Context#session`** — public reader so `Job` (a `Context` subclass) can expose its owning Session to dispatch code.

## [0.1.0] — 2026-04-26

Initial release.

### Added

- **Layered single-file architecture** (`lib/mycel.rb`) under the `Mycel` namespace:
  - `Mycel::Callbacks` — cross-cutting mixin for dynamic event handlers
  - `Mycel::Framing` — Layer 1: length-prefixed binary chunks (up to ~1 GiB) over an IO
  - `Mycel::Transport` — Layer 2: TCP connection lifecycle as composable mixins, plus 13 pre-composed connector classes (`Client`, `Server`, `Servant`, with `Callback`, `Async`, and `Auto` variants)
  - `Mycel::Channel` — Layer 3: multiplexed Command/Job sessions over a framed IO; `Session`, `Hub`, `Command`, `Job`, `Message`, `Context`
  - `Mycel::RPC` — Layer 4: method-name based RPC; `Peer` and `Endpoint` facade
  - `Mycel::Codec::JSON` — pluggable serialiser; swap in MessagePack, Protocol Buffers, etc.
  - `Mycel::ThreadPool` — optional fixed-size executor for high-frequency workloads
- **Perfect role symmetry** — once connected, either side can register methods *and* invoke methods on the other. There is no client/server distinction at the protocol level.
- **Native exception propagation** — remote `raise` is rescued on the caller as the same exception class.
- **Sync (`call_remote`) and async (`call_async`) APIs.**
- **Broadcast** — invoke a method on every connected peer of a given role.
- **Backpressure** — `Session.new(io, max_concurrent_jobs: N)` bounds concurrent inbound work via a counting semaphore.
- **Idempotent close** — sessions and hubs survive concurrent close calls; `callback(:close)` fires exactly once.
- **Protocol versioning** — every message carries a `'v'` field; mismatching versions surface as `:version_mismatch` rather than silent corruption.
- **Symbol or String** method names accepted everywhere.
- **Zero monkey-patching of Ruby standard classes** — no `class IO` open, no `module Socket::TCP` open. Everything lives under `Mycel::*`.
- **132-example RSpec suite** covering Framing, Transport, Channel, RPC, integration scenarios, edge cases (UTF-8, large payloads, churn), backpressure, codec swaps, idempotent close, protocol versioning, and `ThreadPool` semantics.

[0.1.1]: https://github.com/firelzrd/mycel/releases/tag/v0.1.1
[0.1.0]: https://github.com/firelzrd/mycel/releases/tag/v0.1.0
