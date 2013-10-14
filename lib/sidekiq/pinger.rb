require 'sidekiq'
require 'sidekiq/util'
require 'celluloid'

module Sidekiq
  module Scheduled
    class Pinger
      include Celluloid
      include Sidekiq::Util

      PING_INTERVAL = 60

      def poll(first_time=false)
        watchdog('scheduling digest poller thread died!') do
          add_jitter if first_time
          logger.debug { "PING #{Time.now.to_s}" }
          STATSD.count("sidekiq.processor")
          after(PING_INTERVAL) { poll }
        end
      end

      def add_jitter
        begin
          sleep(PING_INTERVAL * rand)
        rescue Celluloid::Task::TerminatedError
          # Hit Ctrl-C when Sidekiq is finished booting and we have a chance
          # to get here.
        end
      end
    end
  end
end
