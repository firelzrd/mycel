# Mycel

A symmetric, fully-duplex RPC library for Ruby.

> *Once a TCP connection is established, the notion of "which side called whom" dissolves. There is no client. There is no server. Only peers, growing toward each other like hyphae in a mycelial mat.*

<div align="center"><img width="320" height="320" alt="e19a5764-cc5c-4bb6-9354-906250ec7538" src="https://github.com/user-attachments/assets/d366676e-85a4-44b1-a4d7-722ad60137c2" /></div>

## Why Mycel?

Most RPC libraries treat one side as a server (passive, exposes methods) and the other as a client (active, makes calls). Mycel doesn't. Both endpoints share the same `Peer` API: each one **registers methods** the other can invoke, and each one **calls methods** the other has registered. The TCP transport is full-duplex; Mycel makes the application layer match.

Each connection is one **hypha**. Each `Endpoint` is a **node**. A node can grow as many hyphae as it wants — listen for incoming connections, dial out to others, or both at once. The result is a programmable mycelial mat of equal peers.

## Installation

Add this line to your application's Gemfile:

```ruby
gem 'mycel'
```

And then execute:

```bash
bundle install
```

Or install it yourself as:

```bash
gem install mycel
```

Pure Ruby, no native extensions, no runtime dependencies beyond the standard library (`socket`, `monitor`, `securerandom`, `json`, `timeout`).

## Quick Start

```ruby
require 'mycel'

# A node that exposes some methods over TCP.
server = Mycel::RPC::Endpoint.new
server.peer.register_method(:echo)   { |msg| msg }
server.peer.register_method(:divide) { |a, b|
  raise ZeroDivisionError, 'b must be non-zero' if b.zero?
  a.fdiv(b)
}
server.peer.register_method(:slow)   { |sec| sleep sec; "slept #{sec}s" }
server.start_server(5345)

# Two clients on the same server. Each registers its OWN method —
# because the channel is bidirectional, the server can call back
# into either of them with no extra plumbing.
spawn_client = ->(label) {
  Mycel::RPC::Endpoint.new.tap { |c|
    c.peer.register_method(:whoami) { label }
    c.connect_to('localhost', 5345)
  }
}
alice = spawn_client.call('alice')
bob   = spawn_client.call('bob')

# Synchronous calls (Symbol method names work, native return values).
alice.call_remote(:echo, 'hi')        # => "hi"
alice.call_remote(:divide, 22, 7)     # => 3.142857142857143

# Remote exceptions raise on the caller as native Ruby exceptions.
begin
  alice.call_remote(:divide, 1, 0)
rescue ZeroDivisionError => e
  puts "rescued: #{e.message}"
end

# Async + callback (caller does not block).
done = Queue.new
alice.call_async(:slow, 0.1) { |err, res| done.enq(err || res) }
done.deq                              # => "slept 0.1s"

# Server-to-clients broadcast over the same duplex channel.
server.broadcast(:whoami, role: :clients).each { |r|
  puts "#{r[:session_id][0, 8]}… answered #{r[:result].inspect}"
}

[alice, bob, server].each(&:shutdown)
```

## Architecture

Mycel is a single file, structured as four layers plus a cross-cutting mixin:

```
Mycel
├── Callbacks         cross-cutting mixin for dynamic event handlers
├── Framing           Layer 1 — length-prefixed binary chunks over an IO
├── Transport         Layer 2 — TCP connection lifecycle (composable mixins)
├── Channel           Layer 3 — multiplexed Command/Job sessions over a framed IO
│   ├── Session       one duplex channel (one TCP socket worth of work)
│   ├── Hub           a collection of sessions (server + client roles)
│   ├── Command       outbound operation (you initiated)
│   ├── Job           inbound operation (peer initiated)
│   └── Message       wire-format builder
├── RPC               Layer 4 — method-name based remote procedure calls
│   ├── Peer          register_method / call (the stateless RPC API)
│   └── Endpoint      thin facade: start_server / connect_to / shutdown
├── Codec             pluggable serialiser (default: JSON; swap for MessagePack etc.)
└── ThreadPool        optional executor for high-frequency workloads
```

The dependency graph is strictly downward: `RPC → Channel → {Framing, Transport}`. `Callbacks` is included wherever events need to be observed.

## Features

- **Perfect symmetry.** Every connection is a duplex RPC channel. Both sides expose methods, both sides invoke them. There is no client/server role at the protocol level — only at the TCP-listen-vs-dial level (which Mycel makes a per-call detail, not an architectural one).
- **Native exception propagation.** A `raise ZeroDivisionError` on one side comes out the other side as a `ZeroDivisionError` you can `rescue` normally.
- **Async + sync.** Use `call_remote` to block until response, or `call_async { |err, res| ... }` to fire and continue.
- **Broadcast.** `endpoint.broadcast(:method, role: :clients)` invokes a method on every connected peer of a given role and gathers results.
- **Backpressure.** `Session.new(io, max_concurrent_jobs: N)` bounds how many incoming requests a session will process concurrently — slow producers cooperate via a counting semaphore.
- **Pluggable codec.** Default is JSON. Drop in MessagePack, Protocol Buffers, or anything responding to `.encode(hash) / .decode(bytes)`.
- **Pluggable executor.** Default is `Thread.new` per request. Plug in `Mycel::ThreadPool.new(size: 16)` (or any object responding to `call(&block)`) to amortise thread-creation overhead.
- **Zero monkey-patching.** Mycel does not open `class IO`, `module Socket::TCP`, or any other Ruby standard namespace. Everything lives under `Mycel::*`.
- **Single file.** The entire library is one self-contained `lib/mycel.rb`. No autoloading magic, no scattered concerns.

## Bidirectional RPC, Concretely

A common pattern: register methods on **both** sides of the same connection.

```ruby
# === In one process ===
node_a = Mycel::RPC::Endpoint.new
node_a.peer.register_method(:ping) { |from| "node-A heard #{from}" }
node_a.start_server(5345)

# === In another process ===
node_b = Mycel::RPC::Endpoint.new
node_b.peer.register_method(:ping) { |from| "node-B heard #{from}" }
node_b.connect_to('localhost', 5345)

# B → A:
node_b.call_remote(:ping, 'B')                          # => "node-A heard B"

# A → B (same connection, opposite direction):
sid = node_a.server_session_ids.first
node_a.peer.call(sid, :ping, 'A')                       # => "node-B heard A"
```

The TCP socket carries traffic in both directions simultaneously. Mycel multiplexes outbound `Command` IDs and inbound `Job` IDs in separate ID pools per session, so nothing collides regardless of how busy the channel gets.

## Running the Demo

```bash
ruby lib/mycel.rb
```

The bottom of `lib/mycel.rb` contains a 40-line tour script demonstrating sync calls, exception propagation, async callbacks, and bidirectional broadcast on a local TCP port.

## Running the Tests

```bash
bundle exec rspec
```

132 examples cover Framing, Transport, Channel, RPC, integration scenarios, edge cases (UTF-8, large payloads, churn), backpressure, codec swaps, idempotent close, protocol versioning, and ThreadPool semantics.

## License

MIT — see [LICENSE](LICENSE).

## Author

Masahito Suzuki ([@firelzrd](https://github.com/firelzrd))
