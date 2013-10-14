require 'sidekiq/util'
require 'sidekiq/manager'
require 'sidekiq/scheduled'
require 'sidekiq/digest_poller'

PING = defined?(STATSD)
require 'sidekiq/pinger' if PING

module Sidekiq
  class Launcher
    attr_reader :manager, :poller, :digest_poller, :pinger, :options
    def initialize(options)
      @options       = options
      @manager       = Sidekiq::Manager.new(options)
      @poller        = Sidekiq::Scheduled::Poller.new
      @digest_poller = Sidekiq::Scheduled::DigestPoller.new
      @pinger        = Sidekiq::Scheduled::Pinger.new if PING
    end

    def run
      manager.async.start
      poller.async.poll(true)
      digest_poller.async.poll(true)
      pinger.async.poll(true) if pinger
    end

    def stop
      poller.async.terminate if poller.alive?
      digest_poller.async.terminate if digest_poller.alive?
      pinger.async.terminate if pinger && pinger.alive?
      manager.async.stop(:shutdown => true, :timeout => options[:timeout])
      manager.wait(:shutdown)
    end

    def procline(tag)
      $0 = manager.procline(tag)
      manager.after(5) { procline(tag) }
    end
  end
end
