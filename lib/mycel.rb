# mycel.rb — single-file RPC library
#
# Layered architecture under the Mycel namespace:
#
#   Mycel::Callbacks   cross-cutting mixin for dynamic event handlers
#   Mycel::Framing     Layer 1 — length-prefixed binary chunks over an IO
#   Mycel::Transport   Layer 2 — TCP connection lifecycle (composable mixins)
#   Mycel::Channel     Layer 3 — multiplexed Command/Job sessions over a framed IO
#   Mycel::RPC         Layer 4 — method-name based remote procedure calls
#
# The library does not monkey-patch Ruby standard classes (no `class IO` open,
# no `module Socket::TCP` open). Everything lives under `Mycel::*`.

require 'socket'
require 'monitor'
require 'securerandom'
require 'json'
require 'timeout'

require_relative 'mycel/version'

module Mycel
  # =========================================================================
  # Cross-cutting: Current session context
  # =========================================================================
  #
  # During execution of a registered RPC method handler (and any code it
  # calls — helpers, services, plugins), `Mycel.current_session` returns the
  # `Mycel::Channel::Session` whose peer initiated the call. Outside such a
  # context (e.g. plain library code, or after the handler returns) it is nil.
  #
  # This is implemented with `Thread.current` rather than a block argument so
  # that handlers retain their existing signatures and any code at any depth
  # can ask "who called me?" without having to thread a session_id through
  # every method. Concurrent calls land on independent threads (mycel spawns
  # a worker per inbound Job by default), so the values do not bleed between
  # peers.
  #
  # Test fixtures and code that wants to invoke a handler outside the normal
  # mycel dispatch path can use `Mycel.with_current_session(fake_session) { ... }`
  # to set the context manually.

  def self.current_session
    Thread.current[:mycel_current_session]
  end

  def self.current_session_id
    current_session&.session_id
  end

  def self.with_current_session(session)
    prev = Thread.current[:mycel_current_session]
    Thread.current[:mycel_current_session] = session
    yield
  ensure
    Thread.current[:mycel_current_session] = prev
  end

  # =========================================================================
  # Cross-cutting: Callbacks
  # =========================================================================
  module Callbacks
    def __mycel_callback_handlers__
      @__mycel_callback_handlers__ ||= Hash.new
    end

    def on(sym, &block)
      raise(ArgumentError) unless sym.is_a?(Symbol)
      __mycel_callback_handlers__[sym] = block
    end

    def callback(sym, ...)
      handler = __mycel_callback_handlers__[sym] ||
                (method((sym.to_s + '_callback').to_sym) rescue nil)
      handler&.call(...)
    end
  end

  # =========================================================================
  # Layer 1: Framing — length-prefixed chunks (up to 1,073,741,823 bytes)
  # =========================================================================
  module Framing
    def self.read(io)
      raise EOFError unless (t1 = io.read(1))
      if ((w = ((t2 = t1.unpack('C')[0]) & 0xC0) >> 6) == 0)
        t3 = 0
      else
        raise EOFError unless (t1 = io.read(w))
        t3 = t1.rjust(4, "\x00").unpack('N')[0]
      end
      io.read(t3 | (t2 & 0x3F) << (w * 8))
    end

    def self.write(io, msg)
      begin
        msg = msg.to_s.force_encoding("ASCII-8BIT")
      rescue NoMethodError
        msg = msg.to_s
      end
      raise(ArgumentError) if (l = msg.length) >= 0xC0000000
      w = ((l & 0x3FFFC000 == 0) ?
        ((l & 0x00003FC0 == 0) ? 0 : 1) :
        ((l & 0x3FC00000 == 0) ? 2 : 3))
      h = Array.new
      w.downto(0) do |i|
        h.push 0xff & (l >> (i * 8))
      end
      h[0] = h[0] | w << 6
      io.print h.pack('C*'), msg
    end

    def read_chunk
      Mycel::Framing.read(self)
    end

    def write_chunk(msg)
      Mycel::Framing.write(self, msg)
    end
  end

  # =========================================================================
  # ThreadPool — fixed-size worker pool, plug-in for Session#executor.
  #
  # The default Job#start strategy is `Thread.new` per request; that's
  # fine for moderate load and is bounded by Session's max_concurrent_jobs.
  # For high-frequency RPC, the per-request thread creation overhead can
  # dominate. Plug a ThreadPool in to amortise that cost:
  #
  #     pool = Mycel::ThreadPool.new(size: 16)
  #     hub  = Mycel::Channel::Hub.new(executor: pool)
  #     # ... later ...
  #     pool.shutdown
  #
  # The pool is intentionally minimal: a Queue, N workers, and a
  # shutdown sentinel. It is NOT a general-purpose executor (no
  # cancellation, no priorities, no return values) — it just satisfies
  # the executor protocol (`call(&block)`) that Session expects.
  #
  # Error semantics: any StandardError that escapes a queued task is
  # **swallowed silently**. This is intentional. A worker that dies on
  # an unhandled exception is unrecoverable — the pool would gradually
  # deplete itself and eventually deadlock the Session. The contract
  # is therefore: *tasks must own their error handling*. In Mycel the
  # only producer of pool tasks is Job#start, which already wraps its
  # body in `begin/rescue` and routes failures through `:error` /
  # response payloads. If you submit your own block via the executor
  # protocol, do the same — assume nothing escapes the pool.
  # =========================================================================
  class ThreadPool
    def initialize(size:)
      raise ArgumentError, "size must be > 0" unless size.is_a?(Integer) && size > 0
      @queue = Queue.new
      @workers = Array.new(size) {
        Thread.new {
          loop {
            task = @queue.deq
            break if task == :__mycel_pool_stop__
            begin
              task.call
            rescue StandardError
              # Tasks are responsible for their own error handling
              # (Job#start wraps in begin/rescue and routes through
              # the :error callback). Anything that escapes here
              # would otherwise terminate the worker, so swallow.
            end
          }
        }
      }
      @stopped = false
      @lock = Monitor.new
    end

    # Executor protocol: hand a block to the pool to run on a worker.
    def call(&block)
      raise "ThreadPool already shut down" if @lock.synchronize { @stopped }
      @queue.enq(block)
    end

    def shutdown
      @lock.synchronize {
        return if @stopped
        @stopped = true
      }
      @workers.size.times { @queue.enq(:__mycel_pool_stop__) }
      @workers.each(&:join)
    end
  end

  # =========================================================================
  # Codec — pluggable Hash <-> bytes serialiser used by Channel::Session.
  # Default is JSON. Drop in MessagePack / Protocol Buffers / etc. by
  # supplying an object responding to .encode(hash) and .decode(bytes).
  # =========================================================================
  module Codec
    module JSON
      def self.encode(hash); ::JSON.generate(hash); end
      def self.decode(bytes); ::JSON.parse(bytes); end
    end
  end

  # =========================================================================
  # Layer 2: Transport — TCP connection mixins and pre-composed classes
  # =========================================================================
  module Transport
    module Mixin
      module Base
        def connect(*args, **named_args)
          args.empty? ? connect_this(**named_args) : connect_to(*args, **named_args)
        end
      end

      module Client
        def connect_to(...)
          TCPSocket.new(...)
        end
      end

      module Server
        def initialize(*args, **named_args)
          peer = case args.length
            when 0 then [nil, 0]
            when 1 then [nil, args[0]]
            when 2 then args
            else raise(ArgumentError)
            end
          @server = TCPServer.new(*peer, **named_args)
        end

        def connect_this
          @server.accept
        end

        def open(*args, **named_args)
          @server.listen(named_args[:backlog] || 0)
        end

        def close
          @server.close
        end

        module Auto
          def initialize(*args, **named_args)
            super
            @thread = nil
            @thread_lock = Monitor.new   # consistent with the rest of the lib
          end

          def open(*args, **named_args)
            @thread_lock.synchronize {
              super
              @thread = Thread.new {
                begin
                  loop { self.connect_this }
                rescue IOError
                end
              }
            }
          end

          def close
            @thread_lock.synchronize {
              super
              @thread.join if @thread && @thread != Thread.current
              @thread = nil
            }
          end
        end
      end

      module Callback
        module Client
          include Mycel::Callbacks
          def connect_to(...)
            sk = TCPSocket.new(...)
            callback(:connect_to, sk)
            callback(:connect, sk)
            sk
          end
        end

        module Server
          include Mycel::Callbacks
          def connect_this
            sk = @server.accept
            callback(:connect_this, sk)
            callback(:connect, sk)
            sk
          end
        end
      end

      module Async
        module Client
          def connect_to(...)
            sk = TCPSocket.new(...)
            Thread.new {
              callback(:connect_to, sk)
              callback(:connect, sk)
            }
            sk
          end
        end

        module Server
          def connect_this
            sk = @server.accept
            Thread.new {
              callback(:connect_this, sk)
              callback(:connect, sk)
            }
            sk
          end
        end
      end
    end

    class Client
      include Mixin::Base
      include Mixin::Client
    end

    class Server
      include Mixin::Base
      include Mixin::Server
    end

    class Servant
      include Mixin::Base
      include Mixin::Client
      include Mixin::Server
    end

    class CallbackClient < Client
      include Mixin::Callback::Client
    end

    class CallbackServer < Server
      include Mixin::Callback::Server
    end

    class CallbackServant < Servant
      include Mixin::Callback::Client
      include Mixin::Callback::Server
    end

    class AsyncClient < CallbackClient
      include Mixin::Async::Client
    end

    class AsyncServer < CallbackServer
      include Mixin::Async::Server
    end

    class AsyncServant < CallbackServant
      include Mixin::Async::Client
      include Mixin::Async::Server
    end

    class AutoServer < CallbackServer
      include Mixin::Server::Auto
    end

    class AutoServant < CallbackServant
      include Mixin::Server::Auto
    end

    class AsyncAutoServer < AutoServer
      include Mixin::Async::Server
    end

    class AsyncAutoServant < AutoServant
      include Mixin::Async::Client
      include Mixin::Async::Server
    end
  end

  # =========================================================================
  # Layer 3: Channel — multiplexed Command/Job sessions
  # =========================================================================
  module Channel
    DIR_COMMAND = :command
    DIR_JOB     = :job

    # Wire-protocol revision. Stamped onto every message so peers can
    # detect incompatible counterparts and refuse with :version_mismatch
    # instead of silently mis-decoding. Bump when the wire format changes
    # in a backward-incompatible way (new mandatory fields, renames,
    # different framing). Receivers default missing 'v' to the current
    # version so a peer running this exact build remains compatible with
    # itself even if the field is stripped en route.
    PROTOCOL_VERSION = 1

    # Wire-message constructors. String keys are used throughout so the
    # same Hash shape survives encode → bytes → decode without symbol/
    # string drift (JSON.parse always yields String keys).
    module Message
      def self.generate_msg(type, id)
        { 'v' => PROTOCOL_VERSION, 'type' => type, 'id' => id }
      end

      def self.generate_payload_msg(type, id, payload)
        { 'v' => PROTOCOL_VERSION, 'type' => type, 'id' => id, 'payload' => payload }
      end
    end

    class Context
      STATE_INIT    = 0
      STATE_RUNNING = 1
      STATE_STOPPED = 2

      class InvalidStateError < RuntimeError; end

      attr_reader :session

      def initialize(session)
        @session = session
        @state = STATE_INIT
        @state_lock = Monitor.new
        @waiting_list = []
      end

      def wait
        q = nil
        @state_lock.synchronize {
          return if @state == STATE_STOPPED
          q = Queue.new
          @waiting_list.push q
        }
        q.deq
      end

      protected

      def guard_assert_state(state, &block)
        @state_lock.synchronize {
          raise(InvalidStateError) if @state != state
          block.call
        }
      end

      def change_state(state)
        @state = state
        @waiting_list.each { |q| q.enq(nil) } if state == STATE_STOPPED
      end
    end

    class Command < Context
      include Mycel::Callbacks

      def initialize(session)
        super
        @id = @session.generate_and_register_out_id(self)
      end

      def request(payload)
        guard_assert_state(STATE_INIT) {
          @session.send(Message.generate_payload_msg('request', @id, payload))
          change_state(STATE_RUNNING)
        }
      end

      def advise(payload)
        guard_assert_state(STATE_RUNNING) {
          @session.send(Message.generate_payload_msg('advise', @id, payload))
        }
      end

      def they_respond(payload)
        guard_assert_state(STATE_RUNNING) {
          callback(:response, payload)
          @session.unregister_id(DIR_COMMAND, @id)
          change_state(STATE_STOPPED)
        }
      end

      def they_abort
        guard_assert_state(STATE_RUNNING) {
          callback(:abort)
          @session.unregister_id(DIR_COMMAND, @id)
          change_state(STATE_STOPPED)
        }
      end

      def they_progress(payload)
        guard_assert_state(STATE_RUNNING) {
          callback(:progress, payload)
        }
      end
    end

    # Inbound side of a single request.
    #
    # Lifecycle hooks (subscribe with `job.on(...)`):
    #
    #   :request  — fired with the request payload when start runs. The
    #               handler executes off main_loop via Session#spawn_handler
    #               (default: a fresh Thread; configurable via Session's
    #               `executor:` parameter).
    #   :advise   — fired when the requester sends a mid-flight advise.
    #   :error    — fired if the :request handler raises StandardError
    #               that escapes user-level rescue. SystemExit and
    #               Interrupt still propagate.
    #
    # Send-side methods (`respond`, `abort`, `progress`) write to the
    # underlying session. They will raise IOError out of Session#send if
    # the session has already closed — call `session.closed?` if you
    # need to bail out gracefully:
    #
    #     j.on(:request) { |payload|
    #       result = compute(payload)
    #       break if @session.closed?
    #       j.respond(result)
    #     }
    #
    # Peer's wrapper handles this for users of the RPC layer; only
    # direct Channel users need to think about it.
    class Job < Context
      include Mycel::Callbacks

      def initialize(session, id)
        super(session)
        @session.register_id(DIR_JOB, id, self)
        @id = id
      end

      # Start the job: transition to RUNNING and dispatch the :request
      # event to the user's handler.
      #
      # The state transition is synchronous so that subsequent messages
      # (advise / abort / etc.) routed by Session#main_loop see
      # STATE_RUNNING. The handler itself runs in a fresh thread so that
      # main_loop is never blocked by user code — a long-running handler
      # would otherwise stall every other in-flight Command and Job on
      # the same session. Callers needing serialisation apply their own
      # mutex/queue inside the handler.
      #
      # Why the rescue catches StandardError and routes it to :error
      # rather than propagating:
      #
      #   * Handler exceptions are a NORMAL part of RPC semantics, not
      #     a bug. The expected protocol is "raise → peer receives an
      #     error response". Peer#handle_method_request wraps the user
      #     method in its own rescue and converts the exception into a
      #     wire-format error response via Job#respond. So in the RPC
      #     case we never reach this rescue at all.
      #
      #   * What we ARE catching here is exceptions that escape Peer's
      #     wrapper — i.e. errors from Channel-direct users (no Peer)
      #     or failures inside Job#respond itself (e.g. IO closed mid-
      #     respond during shutdown). For those cases we still want to
      #     give observers a chance to react without polluting stderr.
      #
      #   * Re-raising would surface via Thread#report_on_exception.
      #     During normal shutdown that prints stack traces to stderr
      #     for benign races (Job#respond losing to a concurrent close).
      #     The :error callback gives the same information to anyone
      #     who actually wants it (loggers, tests, supervisors) without
      #     the noise.
      #
      #   * StandardError (not Exception) so that SystemExit / Interrupt
      #     / SignalException still propagate and let the process exit
      #     cleanly.
      def start(...)
        guard_assert_state(STATE_INIT) {
          change_state(STATE_RUNNING)
        }
        @session.spawn_handler {
          begin
            callback(:request, ...)
          rescue StandardError => e
            callback(:error, e)
          end
        }
      end

      def respond(payload)
        guard_assert_state(STATE_RUNNING) {
          @session.send(Message.generate_payload_msg('response', @id, payload))
          @session.unregister_id(DIR_JOB, @id)
          change_state(STATE_STOPPED)
        }
      end

      def abort
        guard_assert_state(STATE_RUNNING) {
          @session.send(Message.generate_msg('abort', @id))
          @session.unregister_id(DIR_JOB, @id)
          change_state(STATE_STOPPED)
        }
      end

      def progress(payload)
        guard_assert_state(STATE_RUNNING) {
          @session.send(Message.generate_payload_msg('progress', @id, payload))
        }
      end

      def they_advise(payload)
        guard_assert_state(STATE_RUNNING) {
          callback(:advise, payload)
        }
      end
    end

    # A multiplexed bidirectional session over a STREAM IO (TCP socket,
    # UNIX socket, etc.). Stream-orientation is intentional: Framing
    # supplies record boundaries on top of an unbounded byte stream.
    #
    # Datagram transports (UDP) need a different abstraction because each
    # datagram is itself the message boundary — a future
    # Mycel::Channel::DatagramSession would mirror this class's public
    # API but bypass Framing and read whole datagrams via recvfrom. That
    # is deliberately not implemented here; it is a future extension
    # point, not a missing feature.
    class Session
      include Mycel::Callbacks

      DIRECTIONS = [DIR_COMMAND, DIR_JOB].freeze

      # @param io [IO-like]               framed bidirectional stream
      # @param codec [#encode, #decode]    wire serializer
      # @param max_concurrent_jobs [Integer, nil]
      #   Backpressure cap: at most N inbound Jobs may be in flight on this
      #   session at once. main_loop blocks reading the next frame when the
      #   limit is reached, which propagates pressure into the TCP receive
      #   buffer and ultimately stalls the producer. nil = unlimited
      #   (legacy behaviour).
      # @param executor [#call, nil]
      #   Strategy for spawning user :request handlers. The callable
      #   receives a block and is responsible for arranging that block to
      #   execute. The default (nil) starts a fresh Thread per request,
      #   which is fine for moderate load and bounded by max_concurrent_jobs.
      #   Pass an Mycel::ThreadPool (or any callable that implements the
      #   same protocol) to switch to a fixed-size worker pool.
      # `session_id` is the Hub-assigned identifier and is set by the Hub
      # after construction (the Session itself does not generate it, so that
      # the Hub remains the sole authority for namespacing IDs across server
      # and client pools). It is `nil` until the Hub assigns it. Code reading
      # `Mycel.current_session_id` from inside an RPC handler will see the
      # assigned value because handler dispatch happens after registration.
      attr_accessor :session_id

      def initialize(io, codec: Mycel::Codec::JSON, max_concurrent_jobs: nil, executor: nil)
        unless codec.respond_to?(:encode) && codec.respond_to?(:decode)
          raise ArgumentError, "codec must respond to :encode and :decode"
        end
        if executor && !executor.respond_to?(:call)
          raise ArgumentError, "executor must respond to :call"
        end

        @session_id = nil
        @io = io
        @codec = codec
        @executor = executor
        @io_write_lock = Monitor.new
        @io_read_lock  = Monitor.new

        @id_pool = DIRECTIONS.to_h { |d| [d, Hash.new] }
        @id_lock = DIRECTIONS.to_h { |d| [d, Monitor.new] }

        @max_concurrent_jobs = max_concurrent_jobs
        @job_slot_lock = Monitor.new
        @job_slot_cond = @job_slot_lock.new_cond
        @active_job_count = 0

        @thread = nil
        @thread_lock = Monitor.new

        @closed = false
        @close_finished = false
        @close_lock = Monitor.new
        @close_cond = @close_lock.new_cond
      end

      # Number of inbound Jobs currently in flight (registered in the JOB
      # pool but not yet responded/aborted). Useful for instrumentation.
      def active_job_count
        @job_slot_lock.synchronize { @active_job_count }
      end

      # Returns true once close has begun on this session, false otherwise.
      # Use this from inside a Job handler to bail out before calling
      # respond / abort / progress, since those operations raise IOError
      # out of Session#send when the underlying IO is gone.
      #
      # Note this returns true as soon as the close winner flips the gate,
      # i.e. potentially BEFORE callback(:close) has fired. That's the
      # correct semantics for "should I attempt further sends?" — once
      # close has started, the wire is on its way out and any send is a
      # losing race.
      def closed?
        @close_lock.synchronize { @closed }
      end

      # Hand a block off to the configured executor (default: a fresh
      # Thread). Used by Job#start to dispatch the user :request handler
      # off main_loop.
      def spawn_handler(&block)
        if @executor
          @executor.call(&block)
        else
          Thread.new(&block)
        end
      end

      def start(...)
        @thread_lock.synchronize {
          @thread ||= Thread.new { main_loop(...) }
        }
      end

      # Idempotent shutdown using a winner/loser model.
      #
      # close is called from two distinct paths that may interleave:
      #
      #   * EXTERNAL: an outside thread invokes session.close. To unblock
      #     main_loop's pending Framing.read it closes @io, which raises
      #     IOError / SystemCallError inside main_loop and triggers the
      #     INTERNAL path below.
      #
      #   * INTERNAL: main_loop's outer rescue catches the IO error
      #     (whether triggered by the EXTERNAL path or by an unsolicited
      #     peer hangup) and invokes attempt_close on the same thread
      #     that runs main_loop.
      #
      # The two paths can race. attempt_close uses an atomic gate so
      # exactly one caller becomes the WINNER and runs the cleanup body
      # (close_body); everyone else is a LOSER.
      #
      # External callers that lose the race must still observe a fully-
      # closed session before returning, so they wait on @close_cond
      # until the winner signals completion. main_loop's rescue, however,
      # MUST NOT wait — if it did and the winner is external, the winner
      # would block in @thread.join (waiting for main_loop to exit) while
      # main_loop blocks on @close_cond (waiting for the winner to
      # signal): a deadlock. The rescue therefore just returns and lets
      # main_loop exit, which lets the winner's @thread.join unblock and
      # finish.
      #
      # callback(:close) fires from the winner, exactly once, after
      # main_loop has fully exited and @thread is cleared. Hub's
      # registered :close handler can safely unregister the session
      # at that point.
      def close
        return if attempt_close
        @close_lock.synchronize {
          @close_cond.wait_until { @close_finished }
        }
      end

      # Returns true if this caller did the close work itself, false if
      # someone else got there first. Called from both the public close
      # entry point and from main_loop's rescue (where the false return
      # means "external close has it; just exit and let it finish").
      def attempt_close
        am_winner = @close_lock.synchronize {
          if @closed
            false
          else
            @closed = true
            true
          end
        }
        return false unless am_winner

        close_body
        @close_lock.synchronize {
          @close_finished = true
          @close_cond.broadcast
        }
        true
      end

      # The actual cleanup. Order matters:
      #   1. Close @io so any pending Framing.read errors out (this is
      #      what makes main_loop notice an EXTERNAL close).
      #   2. Zero the backpressure counter and broadcast — unsticks
      #      main_loop if it was parked inside acquire_job_slot.
      #   3. Join main_loop (if we are not it) so we know the receive
      #      thread has fully unwound before we declare the session
      #      closed.
      #   4. Clear @thread and fire :close so observers (Hub) can
      #      unregister.
      #
      # Re. `@io.close rescue IOError`:
      #
      #   In Ruby, IO#close on an already-closed stream raises IOError
      #   ("closed stream"). That can happen here because:
      #
      #     a) main_loop's outer rescue path closes @io defensively, and
      #        the EXTERNAL caller may then find @io already gone.
      #     b) An end-user's :request handler (running on its own thread)
      #        races with close. If it tries job.respond after we close
      #        @io, Session#send will see the closed stream and raise.
      #
      #   The rescue swallows that benign duplicate-close. A consequence
      #   that callers should be aware of:
      #
      #     ┃ Job#respond, Job#abort, and Job#progress called AFTER the
      #     ┃ session has closed will raise IOError out of Session#send.
      #     ┃ Peer wraps user-method invocations and routes the failure
      #     ┃ to its own respond path; for direct Channel users, expect
      #     ┃ to rescue IOError or check session liveness yourself.
      #
      #   We don't go further (e.g. queueing late respond payloads) on
      #   purpose: once the session is closed, the wire is gone, and
      #   pretending otherwise just defers failure.
      def close_body
        @io.close rescue IOError
        @job_slot_lock.synchronize {
          @active_job_count = 0
          @job_slot_cond.broadcast
        }
        @thread.join if @thread && @thread != Thread.current
        @thread_lock.synchronize {
          @thread = nil
        }
        callback(:close)
      end

      def generate_and_register_out_id(command_obj)
        raise(ArgumentError) unless command_obj.is_a?(Command)
        id = nil
        @id_lock[DIR_COMMAND].synchronize {
          1.times {
            id = SecureRandom.hex
            redo if @id_pool[DIR_COMMAND].has_key?(id)
            self.register_id(DIR_COMMAND, id, command_obj)
          }
        }
        id
      end

      def register_id(direction, id, obj)
        raise(ArgumentError) unless obj.is_a?(Context)
        @id_lock[direction].synchronize {
          @id_pool[direction][id] = obj
        }
      end

      def unregister_id(direction, id)
        removed = @id_lock[direction].synchronize {
          @id_pool[direction].delete(id)
        }
        # When an inbound Job leaves the pool (via respond/abort), free up
        # one backpressure slot.
        release_job_slot if removed && direction == DIR_JOB
      end

      def send(msg)
        @io_write_lock.synchronize {
          Mycel::Framing.write(@io, @codec.encode(msg))
        }
      end

      protected

      def main_loop(job_class: Job)
        loop {
          bytes = @io_read_lock.synchronize { Mycel::Framing.read(@io) }

          msg = begin
            @codec.decode(bytes)
          rescue StandardError => e
            callback(:protocol_error, :decode_failed, bytes, e)
            next
          end

          unless msg.is_a?(Hash) && msg['type'].is_a?(String) && msg['id'].is_a?(String)
            callback(:protocol_error, :invalid_message, msg)
            next
          end

          # Version policy:
          #
          #   * If 'v' is present and matches PROTOCOL_VERSION → accept.
          #   * If 'v' is present but does NOT match → refuse and surface
          #     :version_mismatch. We do not attempt cross-version fallback
          #     because the wire shape may differ in incompatible ways.
          #   * If 'v' is ABSENT → treat as PROTOCOL_VERSION.
          #
          # The "absent → current" rule exists for two reasons. First, a
          # peer running this same build always self-stamps the field, so
          # absence implies an unstamped path (a hand-crafted test packet,
          # an external producer that hasn't migrated, etc.) where rigid
          # rejection would be more annoying than useful. Second, it lets
          # this build coexist with any pre-versioning peer that might
          # exist in someone's lab without bumping the major version.
          # Future incompatible changes MUST raise PROTOCOL_VERSION; once
          # raised, the absent-field tolerance no longer hides drift
          # because mismatched explicit versions still error out.
          version = msg['v'] || PROTOCOL_VERSION
          unless version == PROTOCOL_VERSION
            callback(:protocol_error, :version_mismatch, version, msg)
            next
          end

          type = msg['type']
          id   = msg['id']

          # Backpressure point: only inbound 'request' messages consume a
          # slot. We acquire AFTER decoding so non-request frames pass
          # through freely, and BEFORE registering the Job so a producer
          # at the cap stalls main_loop here. The next Framing.read won't
          # run until this acquire returns, which propagates pressure into
          # the TCP receive buffer.
          acquire_job_slot if type == 'request'

          case type
          when 'request'  then handle_request(job_class, id, msg['payload'])
          when 'response' then dispatch_to(DIR_COMMAND, id, :they_respond, msg['payload'])
          when 'abort'    then dispatch_to(DIR_COMMAND, id, :they_abort)
          when 'progress' then dispatch_to(DIR_COMMAND, id, :they_progress, msg['payload'])
          when 'advise'
            dispatch_to(DIR_JOB, id, :they_advise, msg['payload']) {
              # No matching Job — the sender thinks the job is still alive
              # but we have already finalised it. Echo back an abort so the
              # sender can clean up its own state.
              self.send(Message.generate_msg('abort', id))
            }
          else
            # Unknown message type. The protocol is intentionally extensible
            # (future heartbeat / streaming / etc. may add new types), so we
            # don't crash — but we do surface the event so observers can
            # decide policy (log, count, drop, terminate). Default with no
            # subscriber is silent forward-skip.
            callback(:protocol_error, :unknown_type, msg)
            next
          end
        }
      rescue IOError, SystemCallError
        # Reaching this rescue means the wire is gone — either because an
        # EXTERNAL close hit @io.close (and our pending Framing.read
        # unblocked with IOError / SystemCallError), or because the peer
        # disconnected unilaterally and we noticed first. We can't tell
        # which from here, but we don't need to: either way, we must
        # finish cleaning up the session.
        #
        # First, fail every in-flight outbound Command. Their owners are
        # blocked in Peer#call / Peer#call_async waiting on a response
        # that will never arrive; they_abort wakes them with
        # ConnectionError so they can unwind.
        @id_lock[DIR_COMMAND].synchronize {
          @id_pool[DIR_COMMAND].each_value(&:they_abort)
        }
        # Then run the close protocol via the same winner/loser gate that
        # the EXTERNAL close uses. Two scenarios:
        #
        #   PEER-INITIATED CLOSE: no one else has called close yet, so
        #     we win the gate and become the cleanup driver. close_body
        #     runs (and @thread.join is skipped because we ARE @thread),
        #     callback(:close) fires, observers unregister.
        #
        #   EXTERNAL-INITIATED CLOSE: an outside thread already entered
        #     close and is parked in @thread.join waiting for us to
        #     exit. We lose the gate and fall straight through. The
        #     rescue body completes, main_loop returns, the thread
        #     dies, the external thread's @thread.join returns, and IT
        #     fires callback(:close).
        #
        # Either way, callback(:close) fires exactly once and Hub-side
        # bookkeeping stays consistent.
        attempt_close
      end

      # Inbound 'request' processing.
      #
      # If the id is fresh, register a new Job and start it. If the id
      # collides with an already-tracked Job, treat it as a duplicate and
      # abort the prior one (the speculatively-acquired backpressure slot
      # is returned, since the new Job won't be created and the prior
      # one's eventual unregister_id will release its own slot).
      def handle_request(job_class, id, payload)
        @id_lock[DIR_JOB].synchronize {
          if @id_pool[DIR_JOB].has_key?(id)
            release_job_slot
            @id_pool[DIR_JOB][id].abort
          else
            job = job_class.new(self, id)
            callback(:job, job)
            job.start(payload)
          end
        }
      end

      # Resolve `id` in the directional pool and forward the call. If no
      # entry exists (typical when a response arrives for a Command that
      # already finalised, or an advise hits a Job that already responded),
      # the optional block runs as the "missing" handler.
      def dispatch_to(direction, id, method, *args)
        @id_lock[direction].synchronize {
          if (op = @id_pool[direction][id])
            op.public_send(method, *args)
          elsif block_given?
            yield
          end
        }
      end

      # acquire_job_slot / release_job_slot together implement a counting
      # semaphore over @active_job_count, capped at @max_concurrent_jobs.
      #
      # When @max_concurrent_jobs is nil (the default) BOTH operations are
      # explicit no-ops. This is by design: callers (main_loop on the
      # acquire side, unregister_id on the release side) invoke them
      # unconditionally. Centralising the "is backpressure active?" check
      # here keeps the call sites uncluttered and guarantees that the
      # acquire/release pair stays balanced regardless of configuration.
      def acquire_job_slot
        return unless @max_concurrent_jobs
        @job_slot_lock.synchronize {
          @job_slot_cond.wait_while { @active_job_count >= @max_concurrent_jobs }
          @active_job_count += 1
        }
      end

      def release_job_slot
        return unless @max_concurrent_jobs
        @job_slot_lock.synchronize {
          @active_job_count -= 1 if @active_job_count > 0
          @job_slot_cond.signal
        }
      end
    end

    # Hub keeps server-role and client-role sessions in separate hashes so the
    # role information has a single source of truth (no duplicated tracking in
    # higher layers).
    class Hub
      include Mycel::Callbacks

      def initialize(server_connector_class: Mycel::Transport::AutoServer,
                     client_connector_class: Mycel::Transport::Client,
                     server_session_class:   Session,
                     client_session_class:   Session,
                     codec:                  Mycel::Codec::JSON,
                     max_concurrent_jobs:    nil,
                     executor:               nil)
        @server_connector_class = server_connector_class
        @client_connector_class = client_connector_class
        @server_session_class   = server_session_class
        @client_session_class   = client_session_class
        @codec                  = codec
        @max_concurrent_jobs    = max_concurrent_jobs
        @executor               = executor

        @server_sessions = Hash.new
        @client_sessions = Hash.new
        @sessions_lock = Monitor.new

        @client_connector = @client_connector_class.new
        @server_connector = nil
        @server_connector_lock = Monitor.new
      end

      def connect(...)
        socket = @client_connector.connect(...)
        session = @client_session_class.new(socket, codec: @codec, max_concurrent_jobs: @max_concurrent_jobs, executor: @executor)
        @sessions_lock.synchronize {
          id = generate_id
          session.session_id = id
          @client_sessions[id] = session
          callback(:client_session, id, session)
          callback(:session, id, session)
          install_close_handler(id, session)
          session.start
        }
      end

      # Hub#open is **not idempotent**. Calling open() while a server
      # connector already exists closes the previous one (tearing down
      # its accept loop and listening socket) before installing the new
      # one. This is deliberate: open(host: 'a') followed by open(host:
      # 'b') must rebind, and the only sensible interpretation of a
      # repeated open() is "start over with these arguments". If you
      # want a guarded variant, check @server_connector yourself before
      # calling, or wrap close+open behind your own intent. Existing
      # accepted sessions on the previous connector are *not* torn down
      # here — they remain in @server_sessions until they close on
      # their own (or the caller invokes Hub#shutdown).
      def open(...)
        @server_connector_lock.synchronize {
          self.close if @server_connector

          connector = @server_connector_class.new(...)
          connector.on(:connect_this) { |socket|
            session = @server_session_class.new(socket, codec: @codec, max_concurrent_jobs: @max_concurrent_jobs, executor: @executor)
            @sessions_lock.synchronize {
              id = generate_id
              session.session_id = id
              @server_sessions[id] = session
              callback(:server_session, id, session)
              callback(:session, id, session)
              install_close_handler(id, session)
              session.start
            }
          }
          connector.open
          @server_connector = connector
        }
      end

      def close
        @server_connector_lock.synchronize {
          @server_connector&.close
          @server_connector = nil
        }
      end

      def get_session(id)
        @sessions_lock.synchronize { @server_sessions[id] || @client_sessions[id] }
      end

      def sessions
        @sessions_lock.synchronize { @server_sessions.merge(@client_sessions) }
      end

      def server_sessions
        @sessions_lock.synchronize { @server_sessions.dup }
      end

      def client_sessions
        @sessions_lock.synchronize { @client_sessions.dup }
      end

      def shutdown
        self.close
        sessions.each_value(&:close)
      end

      protected

      def generate_id
        1.times {
          id = SecureRandom.hex
          redo if @server_sessions.has_key?(id) || @client_sessions.has_key?(id)
          return id
        }
      end

      def install_close_handler(id, session)
        session.on(:close) { unregister(id) }
      end

      def unregister(id)
        @sessions_lock.synchronize {
          @server_sessions.delete(id)
          @client_sessions.delete(id)
        }
      end
    end
  end

  # =========================================================================
  # Layer 4: RPC — method-name based calls over a Channel::Hub
  # =========================================================================
  module RPC
    class Error < StandardError; end
    class ConnectionError    < Error; end
    class MethodNotFoundError < Error; end
    class TimeoutError       < Error; end

    class Peer
      def initialize(hub, config = {})
        @hub = hub
        @method_registry  = {}
        @service_registry = {}
        @config = config
        @response_timeout = config[:timeout] || 30
        setup_session_handlers
      end

      # Method / service names are stored and transmitted as Strings so the
      # wire form (always String after JSON.parse) lines up with the local
      # registry. Symbol arguments are normalised here so callers can write
      # whichever feels natural — `register_method(:add)` and
      # `call(sid, :add)` work the same as their String forms.
      def register_method(name, &block)
        @method_registry[name.to_s] = block
      end

      def register_service(name, service_instance)
        @service_registry[name.to_s] = service_instance
      end

      def call(session_id, method_name, *params, timeout: nil)
        timeout ||= @response_timeout

        payload = {
          "method"    => method_name.to_s,
          "params"    => params,
          "timestamp" => Time.now.to_f
        }

        session = get_session(session_id)
        cmd = Mycel::Channel::Command.new(session)

        response_queue = Queue.new
        cmd.on(:response) { |p| response_queue.enq(p) }
        cmd.on(:abort) {
          response_queue.enq({
            "status" => "error",
            "error"  => { "class" => "ConnectionError", "message" => "Method call was aborted" }
          })
        }

        cmd.request(payload)

        begin
          Timeout.timeout(timeout) do
            response = response_queue.deq
            handle_response(response)
          end
        rescue ::Timeout::Error
          raise TimeoutError, "Method call timed out after #{timeout} seconds"
        end
      end

      def call_async(session_id, method_name, *params, timeout: nil, &callback)
        timeout ||= @response_timeout

        payload = {
          "method"    => method_name.to_s,
          "params"    => params,
          "timestamp" => Time.now.to_f
        }

        session = get_session(session_id)
        cmd = Mycel::Channel::Command.new(session)

        # Single-shot finaliser shared by response / abort / timeout. The
        # ConditionVariable lets the timeout thread wake up the moment the
        # call resolves, so a fast call doesn't strand a thread sleeping for
        # the full timeout window.
        finalised = false
        finalise_lock = Monitor.new
        finalise_cond = finalise_lock.new_cond
        finalise = ->(err, res) {
          fired = false
          finalise_lock.synchronize {
            unless finalised
              finalised = true
              fired = true
              finalise_cond.signal
            end
          }
          callback&.call(err, res) if fired
        }

        cmd.on(:response) { |p|
          begin
            finalise.call(nil, handle_response(p))
          rescue => e
            finalise.call(e, nil)
          end
        }
        cmd.on(:abort) {
          finalise.call(ConnectionError.new("Method call was aborted"), nil)
        }

        Thread.new {
          finalise_lock.synchronize {
            finalise_cond.wait(timeout) unless finalised
          }
          finalise.call(TimeoutError.new("Method call timed out after #{timeout} seconds"), nil)
        }

        cmd.request(payload)
        cmd
      end

      def broadcast_method(method_name, *params, session_ids: nil, timeout: 10)
        ids = session_ids || @hub.sessions.keys
        ids.map { |id|
          begin
            { session_id: id, status: 'success', result: call(id, method_name, *params, timeout: timeout) }
          rescue => e
            { session_id: id, status: 'error', error: e.message }
          end
        }
      end

      private

      def setup_session_handlers
        @hub.on(:session) { |_id, session|
          session.on(:job) { |job|
            job.on(:request) { |payload| handle_method_request(job, payload) }
          }
        }
      end

      def handle_method_request(job, payload)
        method_name = payload["method"]
        params      = payload["params"] || []

        begin
          result = Mycel.with_current_session(job.session) do
            execute_method(method_name, params)
          end
          job.respond(ok_response(result))
        rescue => e
          job.respond(error_response(e))
        end
      end

      def execute_method(name, params)
        if (block = @method_registry[name])
          return block.call(*params)
        end

        service_name, method_name = name.to_s.split('.', 2)
        if service_name && method_name && (service = @service_registry[service_name])
          if service.respond_to?(method_name)
            # public_send: defence in depth — respond_to? already excludes
            # private methods by default, but public_send refuses them at
            # the call site too in case the service has been monkey-patched.
            return service.public_send(method_name, *params)
          end
        end

        raise MethodNotFoundError, "Method not found: #{name}"
      end

      # Wire-format response builders. Two disjoint shapes:
      #
      #   success: { "status" => "ok",    "value" => <returned object> }
      #   error  : { "status" => "error", "error" => { "class" => ...,
      #                                                "message" => ...,
      #                                                "backtrace" => [...] } }
      #
      # The previous schema overloaded "result" to mean "return value" on
      # success and "error class name" on failure; splitting them removes
      # that ambiguity at the cost of a single wire-format break.
      def ok_response(value)
        { "status" => "ok", "value" => value }
      end

      def error_response(exception)
        {
          "status" => "error",
          "error"  => {
            "class"     => exception.class.name,
            "message"   => exception.message,
            "backtrace" => exception.backtrace&.first(5)
          }
        }
      end

      def handle_response(response)
        case response["status"]
        when "ok"
          response["value"]
        when "error"
          err = response["error"] || {}
          raise build_remote_error(err["class"], err["message"])
        else
          raise Error, "Unknown response status: #{response["status"].inspect}"
        end
      end

      # Reconstruct a remote-side exception locally. The class name received
      # over the wire is treated as untrusted; only names that pass ALL of
      # the following gates are reconstructed:
      #
      #   - String shaped like `Foo` or `Foo::Bar::Baz` — each segment must
      #     start with [A-Z]. This explicitly rejects a leading `::` (root
      #     prefix), lowercase identifiers, and anything containing
      #     whitespace, dots, or punctuation.
      #   - Each segment resolves via `const_get(name, false)` (no inherited
      #     lookup), starting from Object.
      #   - The terminal constant is a Class and a StandardError descendant.
      #
      # Anything that fails any of these checks collapses into a generic
      # Mycel::RPC::Error, with the original class name preserved in the
      # message. A malicious or buggy peer therefore cannot drive arbitrary
      # const_get / .new on this side.
      def build_remote_error(class_name, message)
        if class_name.is_a?(String) && class_name.match?(/\A[A-Z][A-Za-z0-9_]*(?:::[A-Z][A-Za-z0-9_]*)*\z/)
          begin
            klass = class_name.split('::').inject(Object) { |ns, c|
              ns.const_get(c, false)
            }
            return klass.new(message) if klass.is_a?(Class) && klass <= StandardError
          rescue NameError
            # fall through to generic Error
          end
        end
        Error.new("#{class_name}: #{message}")
      end

      def get_session(session_id)
        session = @hub.get_session(session_id)
        raise ConnectionError, "Session not found: #{session_id}" unless session
        session
      end
    end

    # Thin facade wrapping a Hub + Peer pair.
    #
    # Endpoint owns the Hub; Peer shares a reference to it (no duplicated
    # session tracking). Role information lives in Hub via server_sessions /
    # client_sessions, so role-aware broadcasts are direct iterations rather
    # than parallel bookkeeping.
    #
    # The convenience methods (call_remote, call_async) target a single client
    # session by default. When more than one client session exists, pass
    # `session_id:` explicitly. For richer connection topologies, drop down to
    # `endpoint.peer` and `endpoint.hub` directly.
    class Endpoint
      attr_reader :peer, :hub, :config

      def initialize(config = {})
        @config = config
        @hub  = Mycel::Channel::Hub.new
        @peer = Peer.new(@hub, config)
      end

      def start_server(port)
        @hub.open(port)
        self
      end

      def connect_to(host, port)
        @hub.connect(host, port)
        self
      end

      def call_remote(method_name, *params, session_id: nil, timeout: nil)
        @peer.call(session_id || sole_client_session_id, method_name, *params, timeout: timeout)
      end

      def call_async(method_name, *params, session_id: nil, timeout: nil, &callback)
        sid = session_id || begin
          sole_client_session_id
        rescue ConnectionError => e
          callback&.call(e, nil) if callback
          raise
        end
        @peer.call_async(sid, method_name, *params, timeout: timeout, &callback)
      end

      def broadcast(method_name, *params, role: :all, timeout: 10)
        ids = case role
              when :all     then @hub.sessions.keys
              when :clients then @hub.server_sessions.keys  # we serve them; they are clients
              when :servers then @hub.client_sessions.keys  # we connect to them; they are servers
              else raise ArgumentError, "role must be :all, :clients, or :servers"
              end
        @peer.broadcast_method(method_name, *params, session_ids: ids, timeout: timeout)
      end

      def server_session_ids
        @hub.server_sessions.keys
      end

      def client_session_ids
        @hub.client_sessions.keys
      end

      def shutdown
        @hub.shutdown
      end

      private

      def sole_client_session_id
        ids = @hub.client_sessions.keys
        raise ConnectionError, "No client connection available"            if ids.empty?
        raise ConnectionError, "Multiple client connections; pass session_id:" if ids.size > 1
        ids.first
      end
    end
  end
end

# ===========================================================================
# Demo — a 40-line tour of what makes Mycel interesting:
#   • symmetry  : every connection is a duplex RPC channel
#   • symbols   : method names work as Symbol or String, your choice
#   • exceptions: remote raises propagate as native Ruby exceptions
#   • async     : non-blocking calls finalise via a callback
#   • broadcast : the server invokes methods exposed by its clients
# ===========================================================================
if $0 == __FILE__
  PORT = 5345

  server = Mycel::RPC::Endpoint.new
  server.peer.register_method(:echo)   { |msg| msg }
  server.peer.register_method(:divide) { |a, b|
    raise ZeroDivisionError, 'b must be non-zero' if b.zero?
    a.fdiv(b)
  }
  server.peer.register_method(:slow)   { |sec| sleep sec; "slept #{sec}s" }
  server.start_server(PORT)

  # Two clients on the same server. Each registers its OWN method —
  # because the channel is bidirectional, the server can call back
  # into either of them with no extra plumbing.
  spawn_client = ->(label) {
    Mycel::RPC::Endpoint.new.tap { |c|
      c.peer.register_method(:whoami) { label }
      c.connect_to('localhost', PORT)
    }
  }
  alice = spawn_client.call('alice')
  bob   = spawn_client.call('bob')

  begin
    puts '─ sync calls (Symbol method names, native return values) ─'
    puts "  echo:   #{alice.call_remote(:echo, 'hi').inspect}"
    puts "  divide: #{alice.call_remote(:divide, 22, 7).round(4)}"

    puts '─ remote exceptions raise on the caller ─'
    begin
      alice.call_remote(:divide, 1, 0)
    rescue ZeroDivisionError => e
      puts "  rescued: #{e.class} — #{e.message}"
    end

    puts '─ async + callback (caller does not block) ─'
    done = Queue.new
    alice.call_async(:slow, 0.1) { |err, res| done.enq(err || res) }
    puts "  callback fired with: #{done.deq.inspect}"

    puts '─ server → clients (broadcast over the same duplex channel) ─'
    server.broadcast(:whoami, role: :clients).each { |r|
      puts "  #{r[:session_id][0, 8]}… answered #{r[:result].inspect}"
    }
  ensure
    [alice, bob, server].each(&:shutdown)
  end
end
