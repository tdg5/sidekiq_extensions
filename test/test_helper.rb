require 'sidekiq'
require 'sidekiq/redis_connection'
require 'minitest/unit'
require 'minitest/autorun'
require 'mocha/setup'

Sidekiq.redis = Sidekiq::RedisConnection.create(:url => "redis://localhost/15", :namespace => 'testacular')
Sidekiq.redis {|connection| connection.flushdb}
