require 'sidekiq'
require 'sidekiq/util'
require 'celluloid'

module Sidekiq
  module Scheduled
    class DigestPoller
      include Celluloid
      include Sidekiq::Util

      def poll(first_time=false)
        watchdog('scheduling digest poller thread died!') do
          add_jitter if first_time

          begin
            to_queue = Sidekiq::DigestibleWorker.descendants

            Sidekiq.redis do |conn|
              now = Time.now
              to_queue.each do |klass|
                key = klass.digestible_key
                process_key = "#{key.gsub(/:pending/, '')}:#{now.strftime('%Y.%m.%d_%H-%M-%S')}"

                if conn.exists(key)
                  conn.rename key, process_key
                  klass.perform_async(process_key)
                end
              end

            end
          rescue SystemCallError, Redis::TimeoutError, Redis::ConnectionError => ex
            # ECONNREFUSED, etc.  Most likely a problem with
            # redis networking.  Punt and try again at the next interval
            logger.warn ex.message
          end

          after(poll_interval) { poll }
        end
      end

      private

      def poll_interval
        60
      end

      def add_jitter
        begin
          sleep(poll_interval * rand)
        rescue Celluloid::Task::TerminatedError
          # Hit Ctrl-C when Sidekiq is finished booting and we have a chance
          # to get here.
        end
      end
    end # BatchPoller
  end # Scheduled

end # Sidekiq
