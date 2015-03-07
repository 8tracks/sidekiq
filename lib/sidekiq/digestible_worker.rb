require 'sidekiq/worker'

module Sidekiq
  class DigestibleWorker
    include Worker
    # include ::NewRelic::Agent::Instrumentation::ControllerInstrumentation

    sidekiq_options :period => 30

    DIGEST_KEY_TTL = 10 * 24 * 60 * 60

    # TODO Move these constants to Worker and allow all sidekiq jobs to
    # auto-snooze/skip
    SKIP          = :_skip_job
    SNOOZE        = :_snooze_job
    RESCHEDULE_IN = 10 * 60 * 60

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

    def self.digestible_rate_limit_key
      "#{self}:rate"
    end

    def self.batch_size_to_pull(redis, rate_key, pending_args_key, pending_key_type, batch, period)
      res = redis.eval(%Q{
        local current_rate
        local items_to_pull
        local rate_key_exists
        local rate_key_ttl
        local pending_args_size

        rate_key_exists = redis.call('EXISTS', KEYS[1])
        rate_key_ttl = redis.call('TTL', KEYS[1])

        if rate_key_exists then
          current_rate = redis.call('GET', KEYS[1])
        else
          current_rate = 0
        end

        if not current_rate then
          current_rate = 0
        end

        if tonumber(current_rate) < tonumber(ARGV[2]) then
          items_to_pull = tonumber(ARGV[2]) - tonumber(current_rate)
        else
          if rate_key_ttl < 0 then
            redis.call("EXPIRE", KEYS[1], tonumber(ARGV[3]))
          end
          return 0
        end

        if ARGV[1] == "list" then
          pending_args_size = redis.call('llen', KEYS[2])
        else
          pending_args_size = redis.call('scard', KEYS[2])
        end

        if tonumber(pending_args_size) <= tonumber(items_to_pull) then
          items_to_pull = tonumber(pending_args_size)
        end

        redis.call('INCRBY', KEYS[1], items_to_pull)
        if not rate_key_exists or rate_key_ttl < 0 then
          redis.call("EXPIRE", KEYS[1], tonumber(ARGV[3]))
        else
          redis.call("EXPIRE", KEYS[1], tonumber(ARGV[3]) - rate_key_ttl)
        end

        return items_to_pull
      }, [ rate_key, pending_args_key ], [ pending_key_type, batch, period ])
    end

    attr_accessor :pending_args_key

    def perform(pending_args_key)
      self.pending_args_key = pending_args_key

      # Pull all args from key
      Sidekiq.redis do |conn|
        # Key could have been evicted or expired
        return unless conn.exists(pending_args_key)

        digest_type = self.class.get_sidekiq_options['digest_type']

        if digest_type == :unique
          @args = conn.smembers(pending_args_key)
        else
          @args = conn.lrange(pending_args_key, 0, -1)
        end

        @args = @args.collect { |a| Sidekiq.load_json(a) }

        action = nil
        if self.class.get_sidekiq_options['grouped']
          group = pending_args_key.split(':')[1]
          action = perform_all(group, @args)
        else
          action = perform_all(@args)
        end

        conn.del(pending_args_key)
      end
    end
    # add_transaction_tracer :perform, :params => '{:pending_args_key => args[0]}'

  end # DigestibleWorker
end
