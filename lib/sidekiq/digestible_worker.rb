require 'sidekiq/worker'
# require 'newrelic_rpm'

module Sidekiq
  class DigestibleWorker
    include Worker
    # include ::NewRelic::Agent::Instrumentation::ControllerInstrumentation

    sidekiq_options :period => 30

    def self.inherited(subclass)
      @digestible_workers ||= []
      @digestible_workers << subclass
      @digestible_workers.uniq!
    end

    def self.descendants
      @digestible_workers ||= []
    end

    def self.digest_perform(*args)
      Sidekiq.redis do |conn|
        if self.get_sidekiq_options['digest_type'] == :unique
# puts "Adding to set: #{digestible_key}"
          conn.sadd(digestible_key, Sidekiq.dump_json(args))
        else
# puts "Adding to list: #{digestible_key}"
          conn.rpush(digestible_key, Sidekiq.dump_json(args))
        end
      end
    end

    def self.digestible_key
      "#{self}:pending"
    end

    # Naive implementation. This assumes the args in redis don't bloat the
    # sidekiq worker too much. If it does, we'll need to split the args into
    # chunks. Also assumes any errors are handled by the #perform_all method.
    def perform(pending_args_key)
      # Pull all args from key
      Sidekiq.redis do |conn|
        if self.class.get_sidekiq_options['digest_type'] == :unique
          @args = conn.smembers(pending_args_key)
        else
          @args = conn.lrange(pending_args_key, 0, -1)
        end

        @args = @args.collect { |a| Sidekiq.load_json(a) }

        perform_all(@args)

        conn.del(pending_args_key)
      end
    end
    # add_transaction_tracer :perform, :params => '{:pending_args_key => args[0]}'

  end # DigestibleWorker
end
