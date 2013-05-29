require 'connection_pool'
require 'redis'

module Sidekiq
  class RedisConnection
    def self.create(options={})
      options[:url] = options[:url] || determine_redis_provider || 'redis://localhost:6379/0' unless options[:sentinel]

      # need a connection for Fetcher and Retry
      size = options[:size] || (Sidekiq.server? ? (Sidekiq.options[:concurrency] + 2) : 5)

      ConnectionPool.new(:timeout => 1, :size => size) do
        if options[:sentinels] && options[:sentinels].length > 0
          build_sentinel_client(options)
        else
          build_client(options[:url], options[:namespace], options[:driver] || 'ruby')
        end
      end
    end

    def self.build_client(url, namespace, driver)
      client = Redis.connect(:url => url, :driver => driver)

      if namespace
        require 'redis/namespace'
        Redis::Namespace.new(namespace, :redis => client)
      else
        client
      end
    end
    private_class_method :build_client

    def self.build_sentinel_client(options)
      client = Redis.new(options.dup)

      if options[:namespace]
        require 'redis/namespace'
        Redis::Namespace.new(options[:namespace], :redis => client)
      else
        client
      end
    end
    private_class_method :build_sentinel_client

    # Not public
    def self.determine_redis_provider
      # REDISTOGO_URL is only support for legacy reasons
      return ENV['REDISTOGO_URL'] if ENV['REDISTOGO_URL']
      provider = ENV['REDIS_PROVIDER'] || 'REDIS_URL'
      ENV[provider]
    end
  end
end
