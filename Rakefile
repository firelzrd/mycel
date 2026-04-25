# frozen_string_literal: true

require "bundler/gem_tasks"
require "rspec/core/rake_task"

RSpec::Core::RakeTask.new(:spec)

desc "Build the gem"
task :build do
  sh "gem build mycel.gemspec"
end

desc "Install the gem locally"
task :install => :build do
  require_relative "lib/mycel/version"
  sh "gem install ./mycel-#{Mycel::VERSION}.gem"
end

desc "Clean build artifacts"
task :clean do
  sh "rm -f *.gem"
end

task :default => :spec
