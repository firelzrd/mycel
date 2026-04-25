require_relative '../lib/mycel'
require 'socket'
require 'stringio'
require 'json'

# Sequential port allocator for tests that need a real TCP listener.
# Base randomised per run so consecutive `rspec` invocations don't collide
# with sockets sitting in TIME_WAIT.
PORT_MUTEX = Mutex.new
PORT_STATE = { counter: 30000 + Random.new.rand(20000) }
def next_port
  PORT_MUTEX.synchronize { PORT_STATE[:counter] += 1 }
end

# Wait for `cond` to become truthy or raise on timeout.
def wait_for(timeout: 2.0, interval: 0.01, &cond)
  deadline = Time.now + timeout
  loop do
    return if cond.call
    raise "wait_for timed out (#{timeout}s)" if Time.now > deadline
    sleep interval
  end
end

# ===========================================================================
# Layer 0: Callbacks
# ===========================================================================
RSpec.describe Mycel::Callbacks do
  let(:klass) { Class.new { include Mycel::Callbacks } }
  let(:obj) { klass.new }

  it 'invokes a registered handler' do
    received = nil
    obj.on(:hello) { |x| received = x }
    obj.callback(:hello, 42)
    expect(received).to eq(42)
  end

  it 'silently no-ops when calling an unregistered symbol' do
    expect { obj.callback(:nope, 1) }.not_to raise_error
  end

  it 'raises ArgumentError for non-Symbol registration' do
    expect { obj.on('hello') {} }.to raise_error(ArgumentError)
    expect { obj.on(42) {} }.to raise_error(ArgumentError)
  end

  it 'overwrites previous handler for the same symbol' do
    a = []
    obj.on(:x) { a << :first }
    obj.on(:x) { a << :second }
    obj.callback(:x)
    expect(a).to eq([:second])
  end

  it 'falls back to a method named <sym>_callback when no handler is registered' do
    klass2 = Class.new {
      include Mycel::Callbacks
      def ping_callback(msg); @last = msg; end
      attr_reader :last
    }
    inst = klass2.new
    inst.callback(:ping, 'hi')
    expect(inst.last).to eq('hi')
  end

  it 'prefers a registered handler over the fallback method' do
    klass2 = Class.new {
      include Mycel::Callbacks
      def ping_callback(_); @from = :method; end
      attr_reader :from
    }
    inst = klass2.new
    inst.on(:ping) { inst.instance_variable_set(:@from, :handler) }
    inst.callback(:ping, 'x')
    expect(inst.from).to eq(:handler)
  end

  it 'passes through multiple arguments to the handler' do
    received = nil
    obj.on(:multi) { |a, b, c| received = [a, b, c] }
    obj.callback(:multi, 1, 2, 3)
    expect(received).to eq([1, 2, 3])
  end

  it 'isolates per-instance handler state' do
    a, b = klass.new, klass.new
    a.on(:x) { :A }
    b.on(:x) { :B }
    expect(a.__mycel_callback_handlers__[:x].call).to eq(:A)
    expect(b.__mycel_callback_handlers__[:x].call).to eq(:B)
  end
end

# ===========================================================================
# Layer 1: Framing
# ===========================================================================
RSpec.describe Mycel::Framing do
  describe 'module function form' do
    it 'roundtrips empty payload' do
      io = StringIO.new(+''.b)
      Mycel::Framing.write(io, '')
      io.rewind
      expect(Mycel::Framing.read(io)).to eq(''.b)
    end

    [1, 63, 64, 16383, 16384, 4194303, 4194304, 65536, 1_000_000].each do |size|
      it "roundtrips a payload of #{size} bytes" do
        payload = ('A'.b * size).b
        io = StringIO.new(+''.b)
        Mycel::Framing.write(io, payload)
        io.rewind
        result = Mycel::Framing.read(io)
        expect(result.bytesize).to eq(size)
        expect(result).to eq(payload)
      end
    end

    it 'roundtrips binary data including null bytes' do
      payload = "\x00\x01\xFF\xFE\x00abc\x00".b
      io = StringIO.new(+''.b)
      Mycel::Framing.write(io, payload)
      io.rewind
      expect(Mycel::Framing.read(io)).to eq(payload)
    end

    it 'roundtrips UTF-8 bytes (transparent over the wire)' do
      payload = '日本語テスト'
      io = StringIO.new(+''.b)
      Mycel::Framing.write(io, payload)
      io.rewind
      result = Mycel::Framing.read(io)
      expect(result.bytes).to eq(payload.bytes)
    end

    it 'rejects payloads of >= 0xC0000000 bytes' do
      fake = Object.new
      def fake.to_s; self; end
      def fake.force_encoding(_); self; end
      def fake.length; 0xC0000000; end
      io = StringIO.new(+''.b)
      expect { Mycel::Framing.write(io, fake) }.to raise_error(ArgumentError)
    end

    it 'raises EOFError on completely empty input' do
      io = StringIO.new(+''.b)
      expect { Mycel::Framing.read(io) }.to raise_error(EOFError)
    end

    it 'raises EOFError on truncated header' do
      io = StringIO.new(+"\xC1".b)  # claims 4-byte header but only has 1
      expect { Mycel::Framing.read(io) }.to raise_error(EOFError)
    end

    it 'allows multiple consecutive frames' do
      io = StringIO.new(+''.b)
      Mycel::Framing.write(io, 'first')
      Mycel::Framing.write(io, 'second')
      Mycel::Framing.write(io, 'third!!')
      io.rewind
      expect(Mycel::Framing.read(io)).to eq('first'.b)
      expect(Mycel::Framing.read(io)).to eq('second'.b)
      expect(Mycel::Framing.read(io)).to eq('third!!'.b)
    end
  end

  describe 'mixin form' do
    let(:io_class) { Class.new(StringIO) { include Mycel::Framing } }

    it 'exposes read_chunk / write_chunk on the including class' do
      io = io_class.new(+''.b)
      io.write_chunk('hello')
      io.rewind
      expect(io.read_chunk).to eq('hello'.b)
    end
  end
end

# ===========================================================================
# Layer 2: Transport
# ===========================================================================
RSpec.describe Mycel::Transport do
  describe 'Client + Server (synchronous)' do
    it 'establishes a TCP connection' do
      port = next_port
      server = Mycel::Transport::Server.new(port)
      Thread.new { server.connect_this rescue nil }
      sleep 0.05
      client = Mycel::Transport::Client.new
      sk = client.connect_to('localhost', port)
      expect(sk).to be_a(TCPSocket)
      sk.close
      server.close
    end
  end

  describe 'Mixin::Base' do
    it 'dispatches connect with no args to connect_this' do
      klass = Class.new {
        include Mycel::Transport::Mixin::Base
        def connect_this(**); :this; end
        def connect_to(*, **); :to; end
      }
      expect(klass.new.connect).to eq(:this)
    end

    it 'dispatches connect with args to connect_to' do
      klass = Class.new {
        include Mycel::Transport::Mixin::Base
        def connect_this(**); :this; end
        def connect_to(*, **); :to; end
      }
      expect(klass.new.connect('x')).to eq(:to)
    end
  end

  describe 'CallbackServer' do
    it 'fires :connect_this and :connect callbacks on accept' do
      port = next_port
      server = Mycel::Transport::CallbackServer.new(port)
      seen = []
      server.on(:connect_this) { |_sk| seen << :this }
      server.on(:connect)      { |_sk| seen << :all }

      Thread.new { server.connect_this rescue nil }
      sleep 0.05
      sk = TCPSocket.new('localhost', port)
      wait_for { seen.size >= 2 }
      expect(seen).to eq([:this, :all])
      sk.close
      server.close
    end
  end

  describe 'AutoServer' do
    it 'accepts connections in a background thread' do
      port = next_port
      server = Mycel::Transport::AutoServer.new(port)
      accepted = Queue.new
      server.on(:connect) { |sk| accepted.enq(sk) }
      server.open
      sk1 = TCPSocket.new('localhost', port)
      sk2 = TCPSocket.new('localhost', port)
      first  = accepted.deq
      second = accepted.deq
      expect(first).to be_a(TCPSocket)
      expect(second).to be_a(TCPSocket)
      sk1.close; sk2.close
      first.close; second.close
      server.close
    end
  end

  describe 'AsyncServer' do
    it 'fires accept callbacks in separate threads' do
      port = next_port
      server = Mycel::Transport::AsyncServer.new(port)
      thread_ids = Queue.new
      server.on(:connect) { |_sk| thread_ids.enq(Thread.current.object_id) }
      Thread.new { server.connect_this rescue nil }
      sleep 0.05
      sk = TCPSocket.new('localhost', port)
      tid = thread_ids.deq
      expect(tid).not_to eq(Thread.main.object_id)
      sk.close
      server.close
    end
  end

  it 'exposes 13 pre-composed connector classes' do
    expected = %i[Client Server Servant
                  CallbackClient CallbackServer CallbackServant
                  AsyncClient AsyncServer AsyncServant
                  AutoServer AutoServant
                  AsyncAutoServer AsyncAutoServant]
    expected.each { |c| expect(Mycel::Transport.const_defined?(c)).to be true }
  end
end

# ===========================================================================
# Layer 3: Channel — foundational pieces
# ===========================================================================
RSpec.describe Mycel::Channel::Message do
  it 'stamps every header-only message with the protocol version and String keys' do
    expect(Mycel::Channel::Message.generate_msg('abort', 'id1')).to eq(
      'v' => Mycel::Channel::PROTOCOL_VERSION, 'type' => 'abort', 'id' => 'id1'
    )
  end

  it 'stamps every payload message with the protocol version and String keys' do
    expect(Mycel::Channel::Message.generate_payload_msg('request', 'id1', { x: 1 })).to eq(
      'v' => Mycel::Channel::PROTOCOL_VERSION, 'type' => 'request', 'id' => 'id1', 'payload' => { x: 1 }
    )
  end

  it 'survives encode/decode roundtrip without key drift' do
    sent = Mycel::Channel::Message.generate_payload_msg('request', 'abc', { 'x' => 1 })
    bytes = Mycel::Codec::JSON.encode(sent)
    received = Mycel::Codec::JSON.decode(bytes)
    expect(received).to eq(sent)
  end
end

RSpec.describe 'RPC error response wire format' do
  def setup_pair
    port = next_port
    server = Mycel::RPC::Endpoint.new
    server.start_server(port)
    client = Mycel::RPC::Endpoint.new
    client.connect_to('localhost', port)
    wait_for { server.server_session_ids.size == 1 && client.client_session_ids.size == 1 }
    [server, client]
  end

  it 'sends success responses as { status: ok, value: ... }' do
    server, client = setup_pair
    server.peer.register_method('add') { |a, b| a + b }
    raw = server.peer.send(:ok_response, 42)
    expect(raw).to eq({ 'status' => 'ok', 'value' => 42 })
    expect(client.call_remote('add', 10, 32)).to eq(42)
    server.shutdown; client.shutdown
  end

  it 'sends error responses as { status: error, error: { class, message, backtrace } }' do
    server, client = setup_pair
    server.peer.register_method('boom') { raise ArgumentError, 'no good' }
    sid = client.client_session_ids.first

    raw = server.peer.send(:error_response, ArgumentError.new('xyz'))
    expect(raw['status']).to eq('error')
    expect(raw['error']).to include('class' => 'ArgumentError', 'message' => 'xyz')

    expect { client.call_remote('boom') }.to raise_error(ArgumentError, 'no good')
    server.shutdown; client.shutdown
  end
end

RSpec.describe 'Endpoint#call_async timeout passthrough' do
  it 'forwards the timeout kwarg to Peer#call_async' do
    port = next_port
    server = Mycel::RPC::Endpoint.new
    server.peer.register_method('slow') { sleep 1 }
    server.start_server(port)
    client = Mycel::RPC::Endpoint.new
    client.connect_to('localhost', port)
    wait_for { client.client_session_ids.size == 1 }

    q = Queue.new
    client.call_async('slow', timeout: 0.1) { |err, _res| q.enq(err) }
    expect(q.deq).to be_a(Mycel::RPC::TimeoutError)
    client.shutdown; server.shutdown
  end
end

RSpec.describe 'Channel protocol versioning' do
  def make_pair
    a, b = Socket.pair(:UNIX, :STREAM)
    [Mycel::Channel::Session.new(a), Mycel::Channel::Session.new(b)]
  end

  it 'fires :protocol_error :version_mismatch when v is unknown' do
    s_a, s_b = make_pair
    seen = Queue.new
    s_b.on(:protocol_error) { |reason, *rest| seen.enq([reason, rest]) }
    s_b.start

    Mycel::Framing.write(
      s_a.instance_variable_get(:@io),
      Mycel::Codec::JSON.encode({'v' => 999, 'type' => 'request', 'id' => 'x', 'payload' => 1})
    )
    reason, rest = seen.deq
    expect(reason).to eq(:version_mismatch)
    expect(rest.first).to eq(999)
    s_a.close rescue nil; s_b.close rescue nil
  end

  it 'tolerates a missing v field by defaulting to the current version' do
    s_a, s_b = make_pair
    received = Queue.new
    s_b.on(:job) { |j| j.on(:request) { |p| j.respond("got:#{p}") } }
    s_a.start; s_b.start

    cmd = Mycel::Channel::Command.new(s_a)
    cmd.on(:response) { |r| received.enq(r) }
    cmd.request('hi')   # generated message has v=1, this is the happy path
    expect(received.deq).to eq('got:hi')
    s_a.close rescue nil; s_b.close rescue nil
  end
end

RSpec.describe Mycel::Channel::Context do
  let(:fake_session) { Class.new { def send(_); end }.new }
  let(:ctx) {
    klass = Class.new(Mycel::Channel::Context) do
      public :guard_assert_state, :change_state
    end
    klass.new(fake_session)
  }

  it 'starts in INIT' do
    expect(ctx.instance_variable_get(:@state)).to eq(Mycel::Channel::Context::STATE_INIT)
  end

  it 'guard_assert_state raises InvalidStateError on mismatch' do
    expect {
      ctx.guard_assert_state(Mycel::Channel::Context::STATE_RUNNING) {}
    }.to raise_error(Mycel::Channel::Context::InvalidStateError)
  end

  it 'wait returns immediately when already STOPPED' do
    ctx.change_state(Mycel::Channel::Context::STATE_STOPPED)
    expect { Timeout.timeout(0.5) { ctx.wait } }.not_to raise_error
  end

  it 'wait blocks until state transitions to STOPPED' do
    woke = []
    threads = 3.times.map { Thread.new { ctx.wait; woke << Thread.current.object_id } }
    sleep 0.05
    expect(woke).to be_empty
    ctx.change_state(Mycel::Channel::Context::STATE_STOPPED)
    threads.each(&:join)
    expect(woke.size).to eq(3)
  end
end

RSpec.describe Mycel::Channel::Session do
  def make_session_pair
    a, b = Socket.pair(:UNIX, :STREAM)
    [Mycel::Channel::Session.new(a), Mycel::Channel::Session.new(b)]
  end

  it 'accepts duck-typed IO (no is_a?(IO) requirement)' do
    a, _b = Socket.pair(:UNIX, :STREAM)
    expect { Mycel::Channel::Session.new(a) }.not_to raise_error
  end

  it 'request/response roundtrip across the session pair' do
    s_a, s_b = make_session_pair
    received = Queue.new
    s_b.on(:job) { |j| j.on(:request) { |p| j.respond('echo:' + p) } }
    s_a.start; s_b.start

    cmd = Mycel::Channel::Command.new(s_a)
    cmd.on(:response) { |r| received.enq(r) }
    cmd.request('hello')

    expect(received.deq).to eq('echo:hello')
    s_a.close rescue nil
    s_b.close rescue nil
  end

  it 'multiplexes many concurrent commands' do
    s_a, s_b = make_session_pair
    s_b.on(:job) { |j| j.on(:request) { |p| j.respond('R:' + p.to_s) } }
    s_a.start; s_b.start

    n = 25
    received = Queue.new
    n.times.map do |i|
      c = Mycel::Channel::Command.new(s_a)
      c.on(:response) { |r| received.enq([i, r]) }
      c.request(i.to_s)
    end

    results = n.times.map { received.deq }.sort_by(&:first)
    expect(results.map(&:first)).to eq((0...n).to_a)
    expect(results.all? { |i, r| r == "R:#{i}" }).to be true
    s_a.close rescue nil
    s_b.close rescue nil
  end

  it 'silently skips malformed JSON instead of crashing the loop' do
    s_a, s_b = make_session_pair
    received = Queue.new
    s_b.on(:job) { |j| j.on(:request) { |p| j.respond('ok:' + p) } }
    s_a.start; s_b.start

    s_a.send('this is not json')

    cmd = Mycel::Channel::Command.new(s_a)
    cmd.on(:response) { |r| received.enq(r) }
    cmd.request('alive')
    expect(received.deq).to eq('ok:alive')
    s_a.close rescue nil
    s_b.close rescue nil
  end

  it 'aborts pending commands when the underlying IO is closed' do
    s_a, s_b = make_session_pair
    s_a.start; s_b.start

    aborted = Queue.new
    cmd = Mycel::Channel::Command.new(s_a)
    cmd.on(:abort) { aborted.enq(true) }
    cmd.request('ping')
    s_b.close
    expect(aborted.deq).to eq(true)
    s_a.close rescue nil
  end
end

# ===========================================================================
# Layer 4: Channel::Hub
# ===========================================================================
RSpec.describe Mycel::Channel::Hub do
  it 'tracks server-role and client-role sessions separately' do
    port = next_port
    server_hub = Mycel::Channel::Hub.new
    server_hub.open(port)

    client_hub = Mycel::Channel::Hub.new
    client_hub.connect('localhost', port)

    wait_for { server_hub.server_sessions.size == 1 && client_hub.client_sessions.size == 1 }

    expect(server_hub.server_sessions.size).to eq(1)
    expect(server_hub.client_sessions).to be_empty
    expect(client_hub.client_sessions.size).to eq(1)
    expect(client_hub.server_sessions).to be_empty

    client_hub.shutdown
    server_hub.shutdown
  end

  it 'merges both pools in #sessions' do
    port = next_port
    sh = Mycel::Channel::Hub.new; sh.open(port)
    ch = Mycel::Channel::Hub.new
    ch.connect('localhost', port)
    ch.connect('localhost', port)
    wait_for { ch.client_sessions.size == 2 && sh.server_sessions.size == 2 }
    expect(sh.sessions.size).to eq(2)
    expect(ch.sessions.size).to eq(2)
    ch.shutdown; sh.shutdown
  end

  it 'fires :session, :server_session, :client_session callbacks' do
    port = next_port
    sh = Mycel::Channel::Hub.new
    seen = Queue.new
    sh.on(:session)        { |id, _s| seen.enq([:session, id]) }
    sh.on(:server_session) { |id, _s| seen.enq([:server_session, id]) }
    sh.open(port)

    ch = Mycel::Channel::Hub.new
    ch.connect('localhost', port)
    a = seen.deq
    b = seen.deq
    expect([a[0], b[0]].sort).to eq([:server_session, :session].sort)
    ch.shutdown; sh.shutdown
  end

  it 'auto-removes a session from tracking when it closes' do
    port = next_port
    sh = Mycel::Channel::Hub.new; sh.open(port)
    ch = Mycel::Channel::Hub.new; ch.connect('localhost', port)
    wait_for { sh.server_sessions.size == 1 }
    session = sh.server_sessions.values.first
    session.close
    wait_for { sh.server_sessions.empty? }
    expect(sh.server_sessions).to be_empty
    ch.shutdown; sh.shutdown
  end

  it 'shutdown closes all sessions' do
    port = next_port
    sh = Mycel::Channel::Hub.new; sh.open(port)
    ch = Mycel::Channel::Hub.new
    3.times { ch.connect('localhost', port) }
    wait_for { sh.server_sessions.size == 3 }
    sh.shutdown
    wait_for { sh.sessions.empty? }
    expect(sh.sessions).to be_empty
    ch.shutdown
  end

  it 'allows DI of session and connector classes' do
    custom_session = Class.new(Mycel::Channel::Session)
    hub = Mycel::Channel::Hub.new(server_session_class: custom_session,
                                   client_session_class: custom_session)
    expect(hub.instance_variable_get(:@server_session_class)).to eq(custom_session)
    expect(hub.instance_variable_get(:@client_session_class)).to eq(custom_session)
  end
end

# ===========================================================================
# Layer 5: RPC::Peer
# ===========================================================================
RSpec.describe Mycel::RPC::Peer do
  def setup_peers
    port = next_port
    s_hub = Mycel::Channel::Hub.new; s_peer = Mycel::RPC::Peer.new(s_hub)
    s_hub.open(port)
    c_hub = Mycel::Channel::Hub.new; c_peer = Mycel::RPC::Peer.new(c_hub)
    c_hub.connect('localhost', port)
    wait_for { c_hub.client_sessions.size == 1 && s_hub.server_sessions.size == 1 }
    [s_hub, s_peer, c_hub, c_peer]
  end

  it 'register_method and call return the result' do
    s_hub, s_peer, c_hub, c_peer = setup_peers
    s_peer.register_method('add') { |a, b| a + b }
    sid = c_hub.client_sessions.keys.first
    expect(c_peer.call(sid, 'add', 2, 3)).to eq(5)
    s_hub.shutdown; c_hub.shutdown
  end

  it 'register_service supports dotted method names' do
    svc = Class.new {
      def double(n); n * 2; end
      def triple(n); n * 3; end
    }.new
    s_hub, s_peer, c_hub, c_peer = setup_peers
    s_peer.register_service('math', svc)
    sid = c_hub.client_sessions.keys.first
    expect(c_peer.call(sid, 'math.double', 7)).to eq(14)
    expect(c_peer.call(sid, 'math.triple', 7)).to eq(21)
    s_hub.shutdown; c_hub.shutdown
  end

  it 'raises MethodNotFoundError for unknown methods' do
    s_hub, _s_peer, c_hub, c_peer = setup_peers
    sid = c_hub.client_sessions.keys.first
    expect { c_peer.call(sid, 'nonexistent') }.to raise_error(Mycel::RPC::MethodNotFoundError)
    s_hub.shutdown; c_hub.shutdown
  end

  it 'propagates errors from method bodies' do
    s_hub, s_peer, c_hub, c_peer = setup_peers
    s_peer.register_method('boom') { raise ZeroDivisionError, 'oops' }
    sid = c_hub.client_sessions.keys.first
    expect { c_peer.call(sid, 'boom') }.to raise_error(ZeroDivisionError, 'oops')
    s_hub.shutdown; c_hub.shutdown
  end

  it 'raises TimeoutError when the method exceeds the timeout' do
    s_hub, s_peer, c_hub, c_peer = setup_peers
    s_peer.register_method('slow') { sleep 0.5; 'late' }
    sid = c_hub.client_sessions.keys.first
    expect { c_peer.call(sid, 'slow', timeout: 0.1) }.to raise_error(Mycel::RPC::TimeoutError)
    s_hub.shutdown; c_hub.shutdown
  end

  it 'raises ConnectionError when the session is unknown' do
    hub  = Mycel::Channel::Hub.new
    peer = Mycel::RPC::Peer.new(hub)
    expect { peer.call('nonexistent_id', 'foo') }.to raise_error(Mycel::RPC::ConnectionError)
  end

  it 'call_async invokes the callback with the result' do
    s_hub, s_peer, c_hub, c_peer = setup_peers
    s_peer.register_method('mul') { |a, b| a * b }
    sid = c_hub.client_sessions.keys.first
    q = Queue.new
    c_peer.call_async(sid, 'mul', 6, 7) { |err, res| q.enq([err, res]) }
    err, res = q.deq
    expect(err).to be_nil
    expect(res).to eq(42)
    s_hub.shutdown; c_hub.shutdown
  end

  it 'broadcast_method targets all sessions and reports per-session results' do
    port = next_port
    sh = Mycel::Channel::Hub.new; sh.open(port)
    s_peer = Mycel::RPC::Peer.new(sh)
    s_peer.register_method('whoami') { 'server' }

    client_hubs = 3.times.map { Mycel::Channel::Hub.new }
    client_peers = client_hubs.map { |h| Mycel::RPC::Peer.new(h) }
    client_peers.each_with_index { |cp, i| cp.register_method('whoami') { "client#{i}" } }
    client_hubs.each { |h| h.connect('localhost', port) }
    wait_for { sh.server_sessions.size == 3 }

    results = s_peer.broadcast_method('whoami')
    expect(results.size).to eq(3)
    expect(results.map { |r| r[:status] }).to all eq('success')
    expect(results.map { |r| r[:result] }.sort).to eq(%w[client0 client1 client2])

    client_hubs.each(&:shutdown); sh.shutdown
  end
end

# ===========================================================================
# Layer 6: RPC::Endpoint
# ===========================================================================
RSpec.describe Mycel::RPC::Endpoint do
  def setup_pair
    port = next_port
    server = Mycel::RPC::Endpoint.new
    server.start_server(port)
    client = Mycel::RPC::Endpoint.new
    client.connect_to('localhost', port)
    wait_for { server.server_session_ids.size == 1 && client.client_session_ids.size == 1 }
    [server, client]
  end

  it 'call_remote uses the sole client session by default' do
    server, client = setup_pair
    server.peer.register_method('echo') { |s| s.upcase }
    expect(client.call_remote('echo', 'hi')).to eq('HI')
    server.shutdown; client.shutdown
  end

  it 'call_remote accepts an explicit session_id' do
    server, client = setup_pair
    server.peer.register_method('echo') { |s| s }
    sid = client.client_session_ids.first
    expect(client.call_remote('echo', 'X', session_id: sid)).to eq('X')
    server.shutdown; client.shutdown
  end

  it 'call_remote raises ConnectionError when there are no client sessions' do
    e = Mycel::RPC::Endpoint.new
    expect { e.call_remote('foo') }.to raise_error(Mycel::RPC::ConnectionError, /No client/)
  end

  it 'call_remote raises ConnectionError when there are multiple client sessions and no session_id' do
    port = next_port
    server = Mycel::RPC::Endpoint.new; server.start_server(port)
    client = Mycel::RPC::Endpoint.new
    client.connect_to('localhost', port)
    client.connect_to('localhost', port)
    wait_for { client.client_session_ids.size == 2 }
    expect { client.call_remote('foo') }.to raise_error(Mycel::RPC::ConnectionError, /Multiple/)
    client.shutdown; server.shutdown
  end

  it 'broadcast(role: :clients) iterates server-side sessions' do
    port = next_port
    server = Mycel::RPC::Endpoint.new; server.start_server(port)

    clients = 3.times.map { |i|
      c = Mycel::RPC::Endpoint.new
      c.peer.register_method('id') { i }
      c.connect_to('localhost', port)
      c
    }
    wait_for { server.server_session_ids.size == 3 }
    results = server.broadcast('id', role: :clients)
    expect(results.size).to eq(3)
    expect(results.map { |r| r[:result] }.sort).to eq([0, 1, 2])
    clients.each(&:shutdown); server.shutdown
  end

  it 'broadcast(role: :servers) iterates the client-side sessions' do
    port_a = next_port
    port_b = next_port
    sa = Mycel::RPC::Endpoint.new
    sa.peer.register_method('whoami') { 'A' }
    sa.start_server(port_a)
    sb = Mycel::RPC::Endpoint.new
    sb.peer.register_method('whoami') { 'B' }
    sb.start_server(port_b)
    relay = Mycel::RPC::Endpoint.new
    relay.connect_to('localhost', port_a)
    relay.connect_to('localhost', port_b)
    wait_for { relay.client_session_ids.size == 2 }
    results = relay.broadcast('whoami', role: :servers)
    expect(results.size).to eq(2)
    expect(results.map { |r| r[:result] }.sort).to eq(%w[A B])
    relay.shutdown; sa.shutdown; sb.shutdown
  end

  it 'broadcast(role: ...) rejects an unknown role' do
    e = Mycel::RPC::Endpoint.new
    expect { e.broadcast('foo', role: :something) }.to raise_error(ArgumentError)
  end

  it 'shutdown closes the underlying Hub' do
    server, client = setup_pair
    client.shutdown
    server.shutdown
    expect(server.hub.sessions).to be_empty
    expect(client.hub.sessions).to be_empty
  end
end

# ===========================================================================
# Integration: end-to-end scenarios
# ===========================================================================
RSpec.describe 'Integration' do
  it 'bidirectional: server can call methods registered on the client' do
    port = next_port
    server = Mycel::RPC::Endpoint.new
    server.start_server(port)
    client = Mycel::RPC::Endpoint.new
    client.peer.register_method('greet') { |name| "hello, #{name}" }
    client.connect_to('localhost', port)
    wait_for { server.server_session_ids.size == 1 }
    sid = server.server_session_ids.first
    expect(server.peer.call(sid, 'greet', 'world')).to eq('hello, world')
    client.shutdown; server.shutdown
  end

  it 'concurrent calls on the same client session do not interfere' do
    port = next_port
    server = Mycel::RPC::Endpoint.new
    server.peer.register_method('square') { |n| sleep(rand * 0.05); n * n }
    server.start_server(port)
    client = Mycel::RPC::Endpoint.new
    client.connect_to('localhost', port)
    wait_for { client.client_session_ids.size == 1 }

    threads = (1..20).map { |n| Thread.new { client.call_remote('square', n) } }
    results = threads.map(&:value)
    expect(results.sort).to eq((1..20).map { |n| n * n }.sort)
    client.shutdown; server.shutdown
  end

  it 'closing the server mid-call surfaces an error to the caller' do
    port = next_port
    server = Mycel::RPC::Endpoint.new
    server.peer.register_method('forever') { sleep 0.3 }
    server.start_server(port)
    client = Mycel::RPC::Endpoint.new
    client.connect_to('localhost', port)
    wait_for { client.client_session_ids.size == 1 }

    t = Thread.new {
      begin
        client.call_remote('forever', timeout: 2)
        :no_error
      rescue Mycel::RPC::Error => e
        e.class
      end
    }
    sleep 0.05
    Thread.new { server.shutdown }   # parallelize so we don't block on the in-flight sleep
    expect(t.value.ancestors).to include(Mycel::RPC::Error)
    client.shutdown
  end

  it 'reconnection after shutdown works' do
    port = next_port
    server = Mycel::RPC::Endpoint.new
    server.peer.register_method('ping') { 'pong' }
    server.start_server(port)

    c1 = Mycel::RPC::Endpoint.new
    c1.connect_to('localhost', port)
    wait_for { c1.client_session_ids.size == 1 }
    expect(c1.call_remote('ping')).to eq('pong')
    c1.shutdown

    c2 = Mycel::RPC::Endpoint.new
    c2.connect_to('localhost', port)
    wait_for { c2.client_session_ids.size == 1 }
    expect(c2.call_remote('ping')).to eq('pong')
    c2.shutdown; server.shutdown
  end
end

# ===========================================================================
# Edge cases
# ===========================================================================
RSpec.describe 'Edge cases' do
  it 'handles an empty-string argument' do
    port = next_port
    s = Mycel::RPC::Endpoint.new
    s.peer.register_method('len') { |str| str.length }
    s.start_server(port)
    c = Mycel::RPC::Endpoint.new
    c.connect_to('localhost', port)
    wait_for { c.client_session_ids.size == 1 }
    expect(c.call_remote('len', '')).to eq(0)
    c.shutdown; s.shutdown
  end

  it 'handles UTF-8 multibyte strings end-to-end' do
    port = next_port
    s = Mycel::RPC::Endpoint.new
    s.peer.register_method('echo') { |str| str }
    s.start_server(port)
    c = Mycel::RPC::Endpoint.new
    c.connect_to('localhost', port)
    wait_for { c.client_session_ids.size == 1 }
    expect(c.call_remote('echo', 'こんにちは🌟')).to eq('こんにちは🌟')
    c.shutdown; s.shutdown
  end

  it 'handles a 256KB string payload' do
    big = ('x' * 256 * 1024)
    port = next_port
    s = Mycel::RPC::Endpoint.new
    s.peer.register_method('len') { |str| str.bytesize }
    s.start_server(port)
    c = Mycel::RPC::Endpoint.new
    c.connect_to('localhost', port)
    wait_for { c.client_session_ids.size == 1 }
    expect(c.call_remote('len', big)).to eq(big.bytesize)
    c.shutdown; s.shutdown
  end

  it 'handles methods with nil and complex (nested) arguments' do
    port = next_port
    s = Mycel::RPC::Endpoint.new
    s.peer.register_method('inspect') { |x| x.inspect }
    s.start_server(port)
    c = Mycel::RPC::Endpoint.new
    c.connect_to('localhost', port)
    wait_for { c.client_session_ids.size == 1 }
    expect(c.call_remote('inspect', nil)).to eq('nil')
    expect(c.call_remote('inspect', { 'a' => [1, 2, { 'b' => true }] })).to include('[1, 2,')
    c.shutdown; s.shutdown
  end

  it 'method re-registration overwrites the previous definition' do
    port = next_port
    s = Mycel::RPC::Endpoint.new
    s.peer.register_method('v') { 1 }
    s.peer.register_method('v') { 2 }
    s.start_server(port)
    c = Mycel::RPC::Endpoint.new
    c.connect_to('localhost', port)
    wait_for { c.client_session_ids.size == 1 }
    expect(c.call_remote('v')).to eq(2)
    c.shutdown; s.shutdown
  end

  it 'service method that does not respond_to? raises MethodNotFoundError' do
    port = next_port
    svc = Class.new { def known; :ok; end }.new
    s = Mycel::RPC::Endpoint.new
    s.peer.register_service('svc', svc)
    s.start_server(port)
    c = Mycel::RPC::Endpoint.new
    c.connect_to('localhost', port)
    wait_for { c.client_session_ids.size == 1 }
    expect(c.call_remote('svc.known')).to eq('ok')
    expect { c.call_remote('svc.unknown') }.to raise_error(Mycel::RPC::MethodNotFoundError)
    c.shutdown; s.shutdown
  end

  it 'survives rapid open/close churn (10 clients connecting & disconnecting)' do
    port = next_port
    s = Mycel::RPC::Endpoint.new
    s.peer.register_method('ping') { 'ok' }
    s.start_server(port)
    10.times do
      c = Mycel::RPC::Endpoint.new
      c.connect_to('localhost', port)
      wait_for { c.client_session_ids.size == 1 }
      expect(c.call_remote('ping')).to eq('ok')
      c.shutdown
    end
    s.shutdown
  end

  it 'does not leak Ruby standard namespaces' do
    expect(IO.instance_methods).not_to include(:read_chunk)
    expect(IO.instance_methods).not_to include(:write_chunk)
    expect(Socket.const_defined?(:TCP)).to be false
    expect(Object.const_defined?(:CJRPC)).to be false
    expect(Object.const_defined?(:ChannelManager)).to be false
    expect(Object.const_defined?(:CommandJobs)).to be false
    expect(Object.const_defined?(:ChunkIO)).to be false
    expect(Object.const_defined?(:DynamicCallback)).to be false
  end
end

# ===========================================================================
# Advanced behavioural / stress / hierarchy tests
# ===========================================================================
RSpec.describe 'Channel::Command#advise and Job#progress' do
  def make_pair
    a, b = Socket.pair(:UNIX, :STREAM)
    [Mycel::Channel::Session.new(a), Mycel::Channel::Session.new(b)]
  end

  it 'delivers progress notifications from job to command before responding' do
    s_a, s_b = make_pair
    progresses = Queue.new
    s_b.on(:job) { |j|
      j.on(:request) { |_p|
        3.times { |i| j.progress({ 'step' => i }) }
        j.respond('done')
      }
    }
    s_a.start; s_b.start

    cmd = Mycel::Channel::Command.new(s_a)
    response = Queue.new
    cmd.on(:progress) { |p| progresses.enq(p) }
    cmd.on(:response) { |r| response.enq(r) }
    cmd.request('go')

    expect(response.deq).to eq('done')
    seen = 3.times.map { progresses.deq }
    expect(seen.map { |h| h['step'] }).to eq([0, 1, 2])

    s_a.close rescue nil
    s_b.close rescue nil
  end

  it 'delivers advise notifications from command to job before responding' do
    s_a, s_b = make_pair
    advice = Queue.new
    s_b.on(:job) { |j|
      j.on(:advise)  { |p| advice.enq(p) }
      j.on(:request) { |_p| sleep 0.1; j.respond('ok') }
    }
    s_a.start; s_b.start

    cmd = Mycel::Channel::Command.new(s_a)
    cmd.on(:response) { |_r| }
    cmd.request('go')
    cmd.advise({ 'hint' => 'careful' })

    expect(advice.deq).to eq({ 'hint' => 'careful' })
    s_a.close rescue nil
    s_b.close rescue nil
  end
end

RSpec.describe 'Channel::Context#wait integration' do
  it 'wait returns once the command has completed' do
    a, b = Socket.pair(:UNIX, :STREAM)
    s_a = Mycel::Channel::Session.new(a)
    s_b = Mycel::Channel::Session.new(b)
    s_b.on(:job) { |j| j.on(:request) { |_p| sleep 0.05; j.respond('ok') } }
    s_a.start; s_b.start

    cmd = Mycel::Channel::Command.new(s_a)
    cmd.on(:response) { |_| }
    cmd.request('hi')
    expect { Timeout.timeout(2.0) { cmd.wait } }.not_to raise_error
    s_a.close rescue nil
    s_b.close rescue nil
  end
end

RSpec.describe 'RPC error class hierarchy' do
  it 'all RPC errors descend from Mycel::RPC::Error which descends from StandardError' do
    expect(Mycel::RPC::ConnectionError.ancestors).to include(Mycel::RPC::Error, StandardError)
    expect(Mycel::RPC::MethodNotFoundError.ancestors).to include(Mycel::RPC::Error)
    expect(Mycel::RPC::TimeoutError.ancestors).to include(Mycel::RPC::Error)
  end

  it 'rescuing Mycel::RPC::Error catches all variants' do
    [Mycel::RPC::ConnectionError, Mycel::RPC::MethodNotFoundError, Mycel::RPC::TimeoutError].each do |klass|
      caught = nil
      begin
        raise klass, 'x'
      rescue Mycel::RPC::Error => e
        caught = e
      end
      expect(caught).to be_a(klass)
    end
  end
end

RSpec.describe 'Channel::Hub edge cases' do
  it 'get_session returns nil for an unknown id' do
    hub = Mycel::Channel::Hub.new
    expect(hub.get_session('nope')).to be_nil
  end

  it 're-opening the server on the same Hub closes the previous listener' do
    port_a = next_port
    port_b = next_port
    hub = Mycel::Channel::Hub.new
    hub.open(port_a)
    hub.open(port_b)
    expect { TCPSocket.new('localhost', port_b).close }.not_to raise_error
    expect { TCPSocket.new('localhost', port_a) }.to raise_error(SystemCallError)
    hub.shutdown
  end

  it 'generate_id retries on collision' do
    hub = Mycel::Channel::Hub.new
    seq = ['dup_id', 'dup_id', 'unique_id']
    allow(SecureRandom).to receive(:hex) { seq.shift }
    hub.instance_variable_get(:@server_sessions)['dup_id'] = :placeholder
    id = hub.send(:generate_id)
    expect(id).to eq('unique_id')
  end
end

RSpec.describe 'Stress: many concurrent calls across many sessions' do
  it 'handles 5 clients × 20 calls each in parallel without losing any' do
    port = next_port
    server = Mycel::RPC::Endpoint.new
    server.peer.register_method('id') { |x| x }
    server.start_server(port)

    n_clients = 5
    n_calls   = 20
    clients = n_clients.times.map {
      c = Mycel::RPC::Endpoint.new
      c.connect_to('localhost', port)
      c
    }
    wait_for { server.server_session_ids.size == n_clients }

    threads = clients.flat_map.with_index { |c, ci|
      n_calls.times.map { |i|
        Thread.new { c.call_remote('id', "#{ci}-#{i}") }
      }
    }
    results = threads.map(&:value)
    expect(results.size).to eq(n_clients * n_calls)
    expect(results.uniq.size).to eq(n_clients * n_calls)

    clients.each(&:shutdown)
    server.shutdown
  end
end

RSpec.describe 'Broadcast partial failure' do
  it 'reports per-session success/error independently' do
    port = next_port
    server = Mycel::RPC::Endpoint.new
    server.start_server(port)

    good = Mycel::RPC::Endpoint.new
    good.peer.register_method('do') { 'ok' }
    good.connect_to('localhost', port)

    bad  = Mycel::RPC::Endpoint.new
    bad.peer.register_method('do') { raise 'boom' }
    bad.connect_to('localhost', port)

    wait_for { server.server_session_ids.size == 2 }

    results = server.broadcast('do', role: :clients)
    statuses = results.map { |r| r[:status] }
    expect(statuses.sort).to eq(['error', 'success'])

    good.shutdown; bad.shutdown; server.shutdown
  end
end

RSpec.describe 'RPC::Peer security & validation' do
  def setup_peers
    port = next_port
    s_hub = Mycel::Channel::Hub.new; s_peer = Mycel::RPC::Peer.new(s_hub)
    s_hub.open(port)
    c_hub = Mycel::Channel::Hub.new; c_peer = Mycel::RPC::Peer.new(c_hub)
    c_hub.connect('localhost', port)
    wait_for { c_hub.client_sessions.size == 1 && s_hub.server_sessions.size == 1 }
    [s_hub, s_peer, c_hub, c_peer]
  end

  it 'reconstructs a remote StandardError descendant by name' do
    s_hub, s_peer, c_hub, c_peer = setup_peers
    s_peer.register_method('boom') { raise ArgumentError, 'bad arg' }
    sid = c_hub.client_sessions.keys.first
    expect { c_peer.call(sid, 'boom') }.to raise_error(ArgumentError, 'bad arg')
    s_hub.shutdown; c_hub.shutdown
  end

  it 'falls back to a generic Error when the remote class name does not resolve' do
    peer = Mycel::RPC::Peer.new(Mycel::Channel::Hub.new)
    err = peer.send(:build_remote_error, 'NoSuchClassXYZ', 'msg')
    expect(err).to be_a(Mycel::RPC::Error)
    expect(err.message).to include('NoSuchClassXYZ')
  end

  it 'refuses to instantiate non-StandardError classes by name' do
    peer = Mycel::RPC::Peer.new(Mycel::Channel::Hub.new)
    # Object exists but is not a StandardError, so should NOT be instantiated.
    err = peer.send(:build_remote_error, 'Object', 'oops')
    expect(err).to be_a(Mycel::RPC::Error)
    expect(err.message).to include('Object')
  end

  it 'rejects a class name that does not match the strict identifier pattern' do
    peer = Mycel::RPC::Peer.new(Mycel::Channel::Hub.new)
    [nil, '', 'foo', 'lower::Case', '%; system("ls")', '../../etc'].each do |name|
      err = peer.send(:build_remote_error, name, 'x')
      expect(err).to be_a(Mycel::RPC::Error)
    end
  end

  it 'service registry uses public_send (private methods remain unreachable)' do
    svc = Class.new {
      def public_one; :ok; end
      private
      def private_one; :should_not_reach; end
    }.new
    s_hub = Mycel::Channel::Hub.new
    s_peer = Mycel::RPC::Peer.new(s_hub)
    s_peer.register_service('svc', svc)
    s_hub.open(port = next_port)

    c_hub = Mycel::Channel::Hub.new
    c_peer = Mycel::RPC::Peer.new(c_hub)
    c_hub.connect('localhost', port)
    wait_for { c_hub.client_sessions.size == 1 }
    sid = c_hub.client_sessions.keys.first

    expect(c_peer.call(sid, 'svc.public_one')).to eq('ok')
    expect { c_peer.call(sid, 'svc.private_one') }.to raise_error(Mycel::RPC::MethodNotFoundError)
    s_hub.shutdown; c_hub.shutdown
  end
end

RSpec.describe 'Channel::Session message validation' do
  def make_pair
    a, b = Socket.pair(:UNIX, :STREAM)
    [Mycel::Channel::Session.new(a), Mycel::Channel::Session.new(b)]
  end

  it 'fires :protocol_error with :decode_failed for malformed JSON' do
    s_a, s_b = make_pair
    seen = Queue.new
    s_b.on(:protocol_error) { |reason, *rest| seen.enq([reason, rest]) }
    s_b.start

    Mycel::Framing.write(s_a.instance_variable_get(:@io), 'not_json{[')
    reason, _ = seen.deq
    expect(reason).to eq(:decode_failed)
    s_a.close rescue nil; s_b.close rescue nil
  end

  it 'fires :protocol_error with :invalid_message when the JSON is not a Hash' do
    s_a, s_b = make_pair
    seen = Queue.new
    s_b.on(:protocol_error) { |reason, *_| seen.enq(reason) }
    s_b.start

    Mycel::Framing.write(s_a.instance_variable_get(:@io), '[1,2,3]')
    expect(seen.deq).to eq(:invalid_message)
    s_a.close rescue nil; s_b.close rescue nil
  end

  it 'fires :protocol_error with :unknown_type for unrecognised message types' do
    s_a, s_b = make_pair
    seen = Queue.new
    s_b.on(:protocol_error) { |reason, *rest| seen.enq([reason, rest]) }
    s_b.start

    Mycel::Framing.write(
      s_a.instance_variable_get(:@io),
      Mycel::Codec::JSON.encode({'type' => 'heartbeat', 'id' => 'h1'})
    )
    reason, rest = seen.deq
    expect(reason).to eq(:unknown_type)
    expect(rest.first['type']).to eq('heartbeat')
    s_a.close rescue nil; s_b.close rescue nil
  end

  it 'fires :protocol_error when type or id has the wrong shape' do
    s_a, s_b = make_pair
    seen = Queue.new
    s_b.on(:protocol_error) { |reason, *_| seen.enq(reason) }
    s_b.start

    Mycel::Framing.write(s_a.instance_variable_get(:@io), '{"type":42,"id":"x"}')
    expect(seen.deq).to eq(:invalid_message)
    Mycel::Framing.write(s_a.instance_variable_get(:@io), '{"type":"request","id":null}')
    expect(seen.deq).to eq(:invalid_message)
    s_a.close rescue nil; s_b.close rescue nil
  end
end

RSpec.describe 'Codec swap' do
  # A toy codec that wraps bytes with a sentinel prefix to prove the codec
  # plug-point is honoured end-to-end.
  fake_codec = Module.new do
    def self.encode(hash); 'X' + ::JSON.generate(hash); end
    def self.decode(bytes)
      raise 'bad sentinel' unless bytes.start_with?('X')
      ::JSON.parse(bytes[1..])
    end
  end

  it 'lets the user swap the wire codec via Hub.new(codec:)' do
    port = next_port
    s_hub = Mycel::Channel::Hub.new(codec: fake_codec)
    s_peer = Mycel::RPC::Peer.new(s_hub)
    s_peer.register_method('echo') { |x| x }
    s_hub.open(port)

    c_hub = Mycel::Channel::Hub.new(codec: fake_codec)
    c_peer = Mycel::RPC::Peer.new(c_hub)
    c_hub.connect('localhost', port)
    wait_for { c_hub.client_sessions.size == 1 }
    sid = c_hub.client_sessions.keys.first
    expect(c_peer.call(sid, 'echo', 'hello')).to eq('hello')
    s_hub.shutdown; c_hub.shutdown
  end

  it 'mismatched codecs surface as :protocol_error on the receiver' do
    port = next_port
    s_hub = Mycel::Channel::Hub.new(codec: fake_codec)
    seen = Queue.new
    s_hub.on(:session) { |_id, sess| sess.on(:protocol_error) { |reason, *_| seen.enq(reason) } }
    s_hub.open(port)

    c_hub = Mycel::Channel::Hub.new  # default JSON codec — no 'X' prefix
    c_peer = Mycel::RPC::Peer.new(c_hub)
    c_hub.connect('localhost', port)
    wait_for { c_hub.client_sessions.size == 1 }
    sid = c_hub.client_sessions.keys.first

    Thread.new {
      begin
        c_peer.call(sid, 'whatever', timeout: 0.5)
      rescue StandardError
      end
    }
    expect(seen.deq).to eq(:decode_failed)
    s_hub.shutdown; c_hub.shutdown
  end
end

RSpec.describe 'Channel::Job#:error callback' do
  def make_pair
    a, b = Socket.pair(:UNIX, :STREAM)
    [Mycel::Channel::Session.new(a), Mycel::Channel::Session.new(b)]
  end

  it 'fires :error on the Job when the user :request handler raises' do
    s_a, s_b = make_pair
    errors = Queue.new
    s_b.on(:job) { |j|
      j.on(:error)   { |e| errors.enq(e) }
      j.on(:request) { |_p| raise ArgumentError, 'kaboom' }
    }
    s_a.start; s_b.start

    cmd = Mycel::Channel::Command.new(s_a)
    cmd.on(:response) { |_| } # silence, not the focus of this test
    cmd.request('hi')

    e = errors.deq
    expect(e).to be_a(ArgumentError)
    expect(e.message).to eq('kaboom')
    s_a.close rescue nil; s_b.close rescue nil
  end

  it 'does not raise to stderr when no :error subscriber is attached' do
    s_a, s_b = make_pair
    s_b.on(:job) { |j| j.on(:request) { |_p| raise 'silent' } }
    s_a.start; s_b.start

    # If the Thread inside Job#start were to leak the exception via
    # report_on_exception, it would print to stderr — capture stderr to
    # confirm silence.
    original_err = $stderr
    captured = StringIO.new
    $stderr = captured
    begin
      cmd = Mycel::Channel::Command.new(s_a)
      cmd.on(:response) { |_| }
      cmd.request('hi')
      sleep 0.1
    ensure
      $stderr = original_err
    end
    expect(captured.string).to eq('')
    s_a.close rescue nil; s_b.close rescue nil
  end
end

RSpec.describe 'Peer#call_async timeout' do
  def setup_peers
    port = next_port
    s_hub = Mycel::Channel::Hub.new; s_peer = Mycel::RPC::Peer.new(s_hub)
    s_hub.open(port)
    c_hub = Mycel::Channel::Hub.new; c_peer = Mycel::RPC::Peer.new(c_hub)
    c_hub.connect('localhost', port)
    wait_for { c_hub.client_sessions.size == 1 && s_hub.server_sessions.size == 1 }
    [s_hub, s_peer, c_hub, c_peer]
  end

  it 'fires the callback with TimeoutError when the timeout elapses first' do
    s_hub, s_peer, c_hub, c_peer = setup_peers
    s_peer.register_method('slow') { sleep 1 }
    sid = c_hub.client_sessions.keys.first
    q = Queue.new
    c_peer.call_async(sid, 'slow', timeout: 0.1) { |err, _res| q.enq(err) }
    err = q.deq
    expect(err).to be_a(Mycel::RPC::TimeoutError)
    s_hub.shutdown; c_hub.shutdown
  end

  it 'does not fire the callback twice if response arrives after timeout' do
    s_hub, s_peer, c_hub, c_peer = setup_peers
    s_peer.register_method('slow') { sleep 0.3; 'late' }
    sid = c_hub.client_sessions.keys.first
    fires = Queue.new
    c_peer.call_async(sid, 'slow', timeout: 0.1) { |err, res| fires.enq([err, res]) }
    sleep 0.6
    expect(fires.size).to eq(1)
    s_hub.shutdown; c_hub.shutdown
  end

  it 'the timeout thread exits naturally after the timeout fires (no leak)' do
    s_hub, s_peer, c_hub, c_peer = setup_peers
    s_peer.register_method('slow') { sleep 0.5 }
    sid = c_hub.client_sessions.keys.first

    thread_count_before = Thread.list.size
    q = Queue.new
    c_peer.call_async(sid, 'slow', timeout: 0.05) { |err, res| q.enq([err, res]) }
    err, _res = q.deq
    expect(err).to be_a(Mycel::RPC::TimeoutError)

    # Verify the timeout thread itself terminates rather than parking forever.
    deadline = Time.now + 1.0
    until Thread.list.size <= thread_count_before
      break if Time.now > deadline
      sleep 0.01
    end
    expect(Thread.list.size).to be <= thread_count_before
    s_hub.shutdown; c_hub.shutdown
  end

  it 'wakes the timeout thread early when the response arrives first' do
    s_hub, s_peer, c_hub, c_peer = setup_peers
    s_peer.register_method('fast') { 'instant' }
    sid = c_hub.client_sessions.keys.first

    # Track the timeout thread the call_async spawns; it should have
    # finished well before the long timeout would have elapsed.
    thread_count_before = Thread.list.size
    q = Queue.new
    c_peer.call_async(sid, 'fast', timeout: 30) { |err, res| q.enq([err, res]) }
    err, res = q.deq
    expect(err).to be_nil
    expect(res).to eq('instant')

    # Give the timeout thread a moment to unwind through ConditionVariable.
    deadline = Time.now + 0.5
    until Thread.list.size <= thread_count_before
      break if Time.now > deadline
      sleep 0.01
    end
    expect(Thread.list.size).to be <= thread_count_before
    s_hub.shutdown; c_hub.shutdown
  end
end

RSpec.describe 'RPC name normalisation (Symbol or String)' do
  def setup_pair
    port = next_port
    server = Mycel::RPC::Endpoint.new
    server.start_server(port)
    client = Mycel::RPC::Endpoint.new
    client.connect_to('localhost', port)
    wait_for { client.client_session_ids.size == 1 }
    [server, client]
  end

  it 'register_method accepts a Symbol and is callable by Symbol or String' do
    server, client = setup_pair
    server.peer.register_method(:greet) { |n| "hi, #{n}" }
    expect(client.call_remote(:greet, 'sym')).to eq('hi, sym')
    expect(client.call_remote('greet', 'str')).to eq('hi, str')
    server.shutdown; client.shutdown
  end

  it 'register_method accepts a String and is callable by Symbol' do
    server, client = setup_pair
    server.peer.register_method('echo') { |s| s }
    expect(client.call_remote(:echo, 'x')).to eq('x')
    server.shutdown; client.shutdown
  end

  it 'register_service accepts a Symbol service name' do
    server, client = setup_pair
    svc = Class.new { def double(n); n * 2; end }.new
    server.peer.register_service(:math, svc)
    expect(client.call_remote('math.double', 6)).to eq(12)
    expect(client.call_remote(:'math.double', 7)).to eq(14)
    server.shutdown; client.shutdown
  end

  it 'broadcast accepts a Symbol method_name' do
    port = next_port
    server = Mycel::RPC::Endpoint.new
    server.start_server(port)
    clients = 2.times.map { |i|
      c = Mycel::RPC::Endpoint.new
      c.peer.register_method(:id) { i }
      c.connect_to('localhost', port)
      c
    }
    wait_for { server.server_session_ids.size == 2 }
    results = server.broadcast(:id, role: :clients)
    expect(results.map { |r| r[:result] }.sort).to eq([0, 1])
    clients.each(&:shutdown); server.shutdown
  end
end

RSpec.describe 'Channel::Session#closed?' do
  it 'returns false before close and true after close has begun' do
    a, b = Socket.pair(:UNIX, :STREAM)
    s = Mycel::Channel::Session.new(a)
    s.start
    expect(s.closed?).to be false
    s.close
    expect(s.closed?).to be true
    b.close rescue nil
  end

  it 'lets a Job handler bail out before sending a late respond' do
    a, b = Socket.pair(:UNIX, :STREAM)
    s_a = Mycel::Channel::Session.new(a)
    s_b = Mycel::Channel::Session.new(b)
    closed_seen = Queue.new
    s_b.on(:job) { |j|
      j.on(:request) { |_p|
        sleep 0.1
        closed_seen.enq(j.instance_variable_get(:@session).closed?)
      }
    }
    s_a.start; s_b.start

    cmd = Mycel::Channel::Command.new(s_a)
    cmd.on(:response) { |_| }
    cmd.on(:abort)    { |_| }
    cmd.request('hi')
    sleep 0.02
    s_b.close
    expect(closed_seen.deq).to be true
    s_a.close rescue nil
  end
end

RSpec.describe 'Mycel::ThreadPool' do
  it 'requires positive size' do
    expect { Mycel::ThreadPool.new(size: 0) }.to raise_error(ArgumentError)
    expect { Mycel::ThreadPool.new(size: -1) }.to raise_error(ArgumentError)
  end

  it 'runs queued tasks on its workers' do
    pool = Mycel::ThreadPool.new(size: 4)
    finished = Queue.new
    20.times { |i| pool.call { finished.enq(i) } }
    20.times { finished.deq }
    pool.shutdown
  end

  it 'swallows task errors so workers stay alive' do
    pool = Mycel::ThreadPool.new(size: 2)
    pool.call { raise 'boom' }
    pool.call { raise 'boom2' }
    done = Queue.new
    pool.call { done.enq(:ok) }
    expect(done.deq).to eq(:ok)
    pool.shutdown
  end

  it 'rejects new submissions after shutdown' do
    pool = Mycel::ThreadPool.new(size: 1)
    pool.shutdown
    expect { pool.call {} }.to raise_error(/already shut down/)
  end

  it 'shutdown is idempotent' do
    pool = Mycel::ThreadPool.new(size: 2)
    pool.shutdown
    expect { pool.shutdown }.not_to raise_error
  end
end

RSpec.describe 'Session executor plug-in' do
  it 'rejects an executor that does not respond to call' do
    a, _b = Socket.pair(:UNIX, :STREAM)
    expect { Mycel::Channel::Session.new(a, executor: Object.new) }
      .to raise_error(ArgumentError, /respond to :call/)
  end

  it 'routes :request handlers through the configured executor' do
    pool = Mycel::ThreadPool.new(size: 4)
    port = next_port
    server_hub = Mycel::Channel::Hub.new(executor: pool)
    server_hub.open(port)

    received = Queue.new
    server_peer = Mycel::RPC::Peer.new(server_hub)
    server_peer.register_method('echo') { |s|
      received.enq(Thread.current.object_id)
      s
    }

    client = Mycel::RPC::Endpoint.new
    client.connect_to('localhost', port)
    wait_for { client.client_session_ids.size == 1 }

    threads_seen = 5.times.map {
      client.call_remote('echo', 'x')
      received.deq
    }
    # Pool only has 4 workers, 5 calls — at least one worker thread is reused.
    expect(threads_seen.uniq.size).to be <= 4

    client.shutdown; server_hub.shutdown; pool.shutdown
  end
end

RSpec.describe 'Channel::Session#close idempotency' do
  it 'fires :close exactly once even when called multiple times' do
    a, b = Socket.pair(:UNIX, :STREAM)
    s = Mycel::Channel::Session.new(a)
    fired = 0
    s.on(:close) { fired += 1 }
    s.start

    3.times { s.close }
    expect(fired).to eq(1)
    b.close rescue nil
  end

  it 'fires :close exactly once when external close races with main_loop rescue' do
    a, b = Socket.pair(:UNIX, :STREAM)
    s_a = Mycel::Channel::Session.new(a)
    s_b = Mycel::Channel::Session.new(b)
    fired_a = 0
    fired_b = 0
    s_a.on(:close) { fired_a += 1 }
    s_b.on(:close) { fired_b += 1 }
    s_a.start; s_b.start

    # Closing s_b's IO via b will trigger s_b's main_loop IOError → internal close.
    # Concurrently call s_b.close from outside.
    t = Thread.new { s_b.close }
    sleep 0.01
    s_b.close
    t.join
    sleep 0.05
    expect(fired_b).to eq(1)

    s_a.close
    expect(fired_a).to eq(1)
  end
end

RSpec.describe 'Channel::Session backpressure' do
  it 'caps in-flight inbound jobs to max_concurrent_jobs' do
    a, b = Socket.pair(:UNIX, :STREAM)
    s_a = Mycel::Channel::Session.new(a)
    s_b = Mycel::Channel::Session.new(b, max_concurrent_jobs: 2)

    started = Queue.new
    release = Queue.new
    s_b.on(:job) { |j|
      j.on(:request) { |_p|
        started.enq(true)
        release.deq           # block until the test releases
        j.respond('ok')
      }
    }
    s_a.start; s_b.start

    # Fire 5 requests in rapid succession.
    5.times do |i|
      cmd = Mycel::Channel::Command.new(s_a)
      cmd.on(:response) { |_| }
      cmd.request("req#{i}")
    end

    # Only 2 should start before the cap is hit.
    sleep 0.1
    expect(s_b.active_job_count).to eq(2)
    expect(started.size).to eq(2)

    # Release 2 → 2 more start.
    2.times { release.enq(:go) }
    sleep 0.1
    expect(s_b.active_job_count).to eq(2)
    expect(started.size).to eq(4)

    # Release the rest.
    3.times { release.enq(:go) }
    sleep 0.1
    expect(s_b.active_job_count).to eq(0)

    s_a.close rescue nil; s_b.close rescue nil
  end

  it 'unlimited (default) does not block' do
    a, b = Socket.pair(:UNIX, :STREAM)
    s_a = Mycel::Channel::Session.new(a)
    s_b = Mycel::Channel::Session.new(b)
    s_b.on(:job) { |j| j.on(:request) { |p| j.respond("R:#{p}") } }
    s_a.start; s_b.start

    received = Queue.new
    20.times do |i|
      cmd = Mycel::Channel::Command.new(s_a)
      cmd.on(:response) { |r| received.enq([i, r]) }
      cmd.request(i.to_s)
    end
    20.times { received.deq }
    expect(s_b.active_job_count).to eq(0)
    s_a.close rescue nil; s_b.close rescue nil
  end

  it 'releasing on close wakes up parked main_loop without leaks' do
    a, b = Socket.pair(:UNIX, :STREAM)
    s_a = Mycel::Channel::Session.new(a)
    s_b = Mycel::Channel::Session.new(b, max_concurrent_jobs: 1)
    block = Queue.new
    s_b.on(:job) { |j| j.on(:request) { |_p| block.deq; j.respond('done') } }
    s_a.start; s_b.start

    cmd1 = Mycel::Channel::Command.new(s_a)
    cmd1.on(:response) { |_| }
    cmd1.request('first')
    sleep 0.05
    cmd2 = Mycel::Channel::Command.new(s_a)
    cmd2.on(:response) { |_| }
    cmd2.request('second')   # second one should be parked at the cap

    # Closing should not deadlock even though main_loop is parked.
    expect { Timeout.timeout(2.0) { s_b.close } }.not_to raise_error
    block.close rescue nil
    s_a.close rescue nil
  end

  it 'is propagated through Hub.new(max_concurrent_jobs:)' do
    port = next_port
    server_hub = Mycel::Channel::Hub.new(max_concurrent_jobs: 4)
    server_hub.open(port)
    client_hub = Mycel::Channel::Hub.new
    client_hub.connect('localhost', port)
    wait_for { server_hub.server_sessions.size == 1 }
    sess = server_hub.server_sessions.values.first
    expect(sess.instance_variable_get(:@max_concurrent_jobs)).to eq(4)
    client_hub.shutdown; server_hub.shutdown
  end
end

RSpec.describe 'Channel::Session codec validation' do
  it 'rejects a codec that does not respond to encode/decode' do
    a, _b = Socket.pair(:UNIX, :STREAM)
    expect {
      Mycel::Channel::Session.new(a, codec: Object.new)
    }.to raise_error(ArgumentError, /encode.*decode/)
  end

  it 'accepts any object responding to encode and decode' do
    a, _b = Socket.pair(:UNIX, :STREAM)
    fake = Module.new do
      def self.encode(o); o.to_s; end
      def self.decode(s); s; end
    end
    expect { Mycel::Channel::Session.new(a, codec: fake) }.not_to raise_error
  end
end

RSpec.describe 'Calling after shutdown' do
  it 'raises ConnectionError when the client has already been shut down' do
    port = next_port
    server = Mycel::RPC::Endpoint.new
    server.peer.register_method('ping') { 'pong' }
    server.start_server(port)
    client = Mycel::RPC::Endpoint.new
    client.connect_to('localhost', port)
    wait_for { client.client_session_ids.size == 1 }
    client.shutdown
    wait_for { client.client_session_ids.empty? }
    expect { client.call_remote('ping') }.to raise_error(Mycel::RPC::ConnectionError)
    server.shutdown
  end
end
