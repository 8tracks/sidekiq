require 'sidekiq/util'
require 'sidekiq/manager'
require 'sidekiq/scheduled'
require 'sidekiq/digest_poller'

module Sidekiq
  class Launcher
    attr_reader :manager, :poller, :digest_poller, :options
    def initialize(options)
      @options = options
      @manager = Sidekiq::Manager.new(options)
      @poller  = Sidekiq::Scheduled::Poller.new
      @digest_poller = Sidekiq::Scheduled::DigestPoller.new
    end

    def run
      manager.async.start
      poller.async.poll(true)
      digest_poller.async.poll(true)
    end

    def stop
      poller.async.terminate if poller.alive?
      manager.async.stop(:shutdown => true, :timeout => options[:timeout])
      manager.wait(:shutdown)
    end

    def procline(tag)
      $0 = manager.procline(tag)
      manager.after(5) { procline(tag) }
    end
  end
end
