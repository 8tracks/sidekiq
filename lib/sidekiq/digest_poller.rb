require 'sidekiq'
require 'sidekiq/util'
require 'celluloid'

module Sidekiq
  module Scheduled
    class DigestPoller
      include Celluloid
      include Sidekiq::Util

      if ::Rails.env.development?
        POLL_INTERVAL = 1
      else
        POLL_INTERVAL = 8
      end

      def poll(first_time=false)
        watchdog('scheduling digest poller thread died!') do
          add_jitter if first_time

          if Sidekiq.options[:tag]
            STATSD.count("sidekiq.#{Sidekiq.options[:tag]}.digest_poller.is_running")
          end

          begin
            @now_string = Time.now.strftime('%Y.%m.%d_%H-%M-%S')

            digestible_jobs = Sidekiq::DigestibleWorker.descendants
            Sidekiq.redis do |conn|
              digestible_jobs.each do |klass|
                frequency = (1.0 / klass.get_sidekiq_options['period'])
                # puts "frequency = #{frequency}"

                if klass.get_sidekiq_options['grouped']
                  number_of_groups_queued = conn.scard("#{klass}:groups")
                  # puts "number_of_groups_queued = #{number_of_groups_queued}"
                  if number_of_groups_queued > 0
                    # if multiplier is 1.5, we will pop 1 half the time and 2 half the time
                    multiplier = (frequency * number_of_groups_queued)
                    number_of_groups_to_pop = multiplier.truncate + (rand < (multiplier - multiplier.truncate) ? 1 : 0)

                    if number_of_groups_to_pop > 0
                      # puts "number_of_groups_to_pop = #{number_of_groups_to_pop}"
                      groups_to_work_on = conn.srandmember("#{klass}:groups", number_of_groups_to_pop)
                      # puts "groups_to_work_on = #{groups_to_work_on}"
                      conn.srem("#{klass}:groups", groups_to_work_on)

                      groups_to_work_on.each do |group|
                        key = klass.digestible_key(group)
                        schedule_pending_job(klass, key, conn)
                      end
                    end
                  end

                else
                  if rand < frequency
                    schedule_pending_job(klass, klass.digestible_key, conn)
                  end
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

      # Schedule a digestible job. Returns key for pending args.
      def self.schedule_pending_job(klass, key, conn, timestamp)
        process_key = "#{key.gsub(/:pending/, '')}:#{timestamp}"

        if conn.exists(key)
          conn.rename(key, process_key)
          if !klass.get_sidekiq_options['critical']
            conn.expire(process_key, DigestibleWorker::DIGEST_KEY_TTL)
          end

          # puts "rename #{key} to #{process_key}"
          klass.perform_async(process_key)

          process_key
        end
      end

      def self.schedule_batched_pending_job(klass, key, conn, timestamp)
        process_key = "#{key.gsub(/:pending/, '')}:#{timestamp}"

        if conn.exists(key)
          batch       = klass.get_sidekiq_options['batch']
          if batch.is_a?(Proc)
            batch = batch.call
          end

          period      = klass.get_sidekiq_options['period']
          digest_type = klass.get_sidekiq_options['digest_type']
          rate_key    = klass.digestible_rate_limit_key

          pending_key_type = if digest_type == :unique
                               "set"
                             else
                               "list"
                             end

          batch_size = klass.batch_size_to_pull(
            conn,
            rate_key,
            key,
            pending_key_type,
            batch,
            period
          )

          if batch_size < 1
            STATSD.count("batched_digestible_worker.#{klass}.did_not_queue.zero_batch")
            return process_key
          end

          # Move batch_size args from key (<class>:pending) to job key (<class>:<timestamp>)
          # This is definitely not efficient.
          if digest_type == :unique
            time = Benchmark.realtime do
              items = conn.srandmember(key, batch_size)
              items.in_groups_of(250, false).each do |batch|
                conn.sadd(process_key, batch)
              end
              conn.sdiffstore(key, key, process_key)
            end
            STATSD.timer("batched_digestible_worker.#{klass}.queue_processing.set", time * 1000)

          else
            time = Benchmark.realtime do
              items = conn.lrange(key, 0, batch_size-1)
              items.in_groups_of(250, false).each do |batch|
                conn.rpush(process_key, batch)
              end
              conn.ltrim(key, batch_size, -1)
            end
            STATSD.timer("batched_digestible_worker.#{klass}.queue_processing.list", time * 1000)
          end

          conn.expire(process_key, DigestibleWorker::DIGEST_KEY_TTL)

          STATSD.count("batched_digestible_worker.#{klass}.queued.batch_count", batch_size)

          klass.perform_async(process_key)
        else
          STATSD.count("batched_digestible_worker.#{klass}.did_not_queue.missing_key")
        end

        process_key
      end

      def schedule_pending_job(klass, key, conn)
        if klass.get_sidekiq_options['batch']
          self.class.schedule_batched_pending_job(klass, key, conn, @now_string)
        else
          self.class.schedule_pending_job(klass, key, conn, @now_string)
        end
      end

      private

      def poll_interval
        # Is dependent on number of workers we're running -- the goal is to
        # have an average of 1 poller run every 1 second.
        POLL_INTERVAL
      end

      def add_jitter
        begin
          sleep(POLL_INTERVAL * rand)
        rescue Celluloid::Task::TerminatedError
          # Hit Ctrl-C when Sidekiq is finished booting and we have a chance
          # to get here.
        end
      end
    end # BatchPoller
  end # Scheduled

end # Sidekiq
