# frozen_string_literal: true

require_relative "lib/mycel/version"

Gem::Specification.new do |spec|
  spec.name    = "mycel"
  spec.version = Mycel::VERSION
  spec.authors = ["Masahito Suzuki"]
  spec.email   = ["firelzrd@gmail.com"]

  spec.summary     = "A symmetric, fully-duplex RPC library for Ruby"
  spec.description = <<~DESC
    Mycel is a single-file, pure-Ruby RPC library built around perfect
    role symmetry: once a TCP connection is established, either side can
    invoke any method exposed by the other — there is no client and no
    server, only peers. Each connection is one hypha; each Endpoint is a
    node in your own programmable mycelial mat. Layered architecture
    (Framing / Transport / Channel / RPC), pluggable codec, backpressure,
    pluggable executor, and zero monkey-patching of Ruby standard classes.
  DESC
  spec.homepage = "https://github.com/firelzrd/mycel"
  spec.license  = "MIT"
  spec.required_ruby_version = ">= 3.0.0"

  spec.metadata["homepage_uri"]    = spec.homepage
  spec.metadata["source_code_uri"] = "https://github.com/firelzrd/mycel"
  spec.metadata["changelog_uri"]   = "https://github.com/firelzrd/mycel/blob/main/CHANGELOG.md"

  spec.files = Dir.chdir(__dir__) do
    `git ls-files -z`.split("\x0").reject do |f|
      (File.expand_path(f) == __FILE__) ||
        f.start_with?(*%w[bin/ test/ spec/ features/ .git .github appveyor Gemfile])
    end
  end

  spec.bindir        = "exe"
  spec.executables   = spec.files.grep(%r{\Aexe/}) { |f| File.basename(f) }
  spec.require_paths = ["lib"]

  # No runtime dependencies — pure Ruby, only stdlib (socket, monitor,
  # securerandom, json, timeout).
end
