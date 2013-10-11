require 'sidekiq/worker'

module Sidekiq
  class DigestibleWorker
    include Worker
    # include ::NewRelic::Agent::Instrumentation::ControllerInstrumentation

    sidekiq_options :period => 30

    DIGEST_KEY_TTL = 10 * 24 * 60 * 60

    def self.inherited(subclass)
      @digestible_workers ||= []
      @digestible_workers << subclass
      @digestible_workers.uniq!
    end

    def self.descendants
      @digestible_workers ||= []
    end

    def self.digest_perform(*args)
      queue_a_job(nil, args)
    end

    def self.grouped_digest_perform(group, *args)
      Sidekiq.redis do |conn|
        conn.pipelined do
          queue_a_job(group, args)

          # queue_group
          # puts "Adding group #{group} to set: #{self}:groups"
          conn.sadd("#{self}:groups", group)
        end
      end
    end

    def self.queue_a_job(group, args)
      key = self.digestible_key(group)

      Sidekiq.redis do |conn|
        if self.get_sidekiq_options['digest_type'] == :unique
  # puts "Adding to set: #{key}"
          conn.sadd(key, Sidekiq.dump_json(args))
        else
  # puts "Adding to list: #{key}"
          conn.rpush(key, Sidekiq.dump_json(args))
        end

        # Set expiry on non-critical jobs to allow redis to evict these keys
        # when redis maxmemory-policy is set to volatile-*.
        if !self.get_sidekiq_options['critical']
          conn.expire(key, DIGEST_KEY_TTL)
        end
      end
    end

    def self.digestible_key(group=nil)
      if group
        "#{self}:#{group}:pending"
      else
        "#{self}:pending"
      end
    end

    # Naive implementation. This assumes the args in redis don't bloat the
    # sidekiq worker too much. If it does, we'll need to split the args into
    # chunks. Also assumes any errors are handled by the #perform_all method.
    def perform(pending_args_key)

      # Pull all args from key
      Sidekiq.redis do |conn|
        # Key could have been evicted or expired
        return unless conn.exists(pending_args_key)

        if self.class.get_sidekiq_options['digest_type'] == :unique
          @args = conn.smembers(pending_args_key)
        else
          @args = conn.lrange(pending_args_key, 0, -1)
        end

        @args = @args.collect { |a| Sidekiq.load_json(a) }

        if self.class.get_sidekiq_options['grouped']
          group = pending_args_key.split(':')[1]
          perform_all(group, @args)
        else
          perform_all(@args)
        end

        conn.del(pending_args_key)
      end
    end
    # add_transaction_tracer :perform, :params => '{:pending_args_key => args[0]}'

  end # DigestibleWorker
end
