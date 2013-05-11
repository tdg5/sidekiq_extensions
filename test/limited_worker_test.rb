require 'test_helper'
require 'sidekiq_extensions/limiter'

class LimitedWorkerTest < MiniTest::Unit::TestCase

	class TestWorker
		include Sidekiq::Worker
	end


	def create_limited_worker
		@limited_worker = SidekiqExtensions::Limiter::LimitedWorker.new(TestWorker.new, @valid_message)
	end


	def setup
		@valid_limit_options = {
			:per_redis => 4,
			:per_queue => 3,
			:per_host => 2,
			:per_process => 1,
			:retry => 2,
			:retry_delay => 1,
		}
		@valid_message = {
			'args' => [],
			'class' => 'test',
			'limits' => @valid_limit_options,
			'queue' => 'elle',
			'retry' => true,
		}
		TestWorker.sidekiq_options_hash = nil
		Sidekiq.redis{|connection| connection.flushdb}
	end


	def test_capacity_availiable_returns_false_if_any_limit_at_capacity
		TestWorker.sidekiq_options(:limits => @valid_limit_options)
		create_limited_worker
		@limited_worker.expects(:scopes_counts).times(2).returns([99, 99, 99, 99])
		Sidekiq.redis do |connection|
			refute @limited_worker.capacity_available?(connection)
		end
	end


	def test_capacity_available_tries_purging_stale_workers
		TestWorker.sidekiq_options(:limits => @valid_limit_options)
		create_limited_worker
		Sidekiq.redis do |connection|
			@limited_worker.scopes_keys.each{|scope_key| connection.sadd(scope_key, 'stale_worker')}
			refute @limited_worker.capacity_available?(connection, true)
			assert @limited_worker.capacity_available?(connection)
		end
	end


	def test_key_uses_custom_worker_key
		TestWorker.sidekiq_options(:limits => {:key => 'test_worker_key'})
		create_limited_worker
		assert_equal 'sidekiq_extensions:limiter:test_worker_key', @limited_worker.key
	end


	def test_limited_scopes_returns_expected_values
		limits = Hash[SidekiqExtensions::Limiter::LimitedWorker::PRIORITIZED_COUNT_SCOPES.map{|scope| [scope, 1]}]
		scopes = limits.keys
		TestWorker.sidekiq_options(:limits => limits)
		until limits.empty?
			create_limited_worker
			assert_equal scopes, @limited_worker.limited_scopes
			TestWorker.sidekiq_options_hash['limits'].delete(scopes.delete(scopes.sample))
		end
	end


	def test_purge_stale_workers_does_not_affect_active_workers
		TestWorker.sidekiq_options(:limits => @valid_limit_options)
		create_limited_worker
		Sidekiq.redis do |connection|
			connection.sadd('workers', 'active_worker')
			@limited_worker.scopes_keys.each{|scope_key| connection.sadd(scope_key, 'active_worker')}
			@limited_worker.purge_stale_workers(connection)
			assert_equal [1, 1, 1, 1], @limited_worker.scopes_counts(connection)
		end
	end


	def test_purge_stale_workers_purges_stale_workers
		TestWorker.sidekiq_options(:limits => @valid_limit_options)
		create_limited_worker
		Sidekiq.redis do |connection|
			@limited_worker.scopes_keys.each{|scope_key| connection.sadd(scope_key, 'stale_worker')}
			@limited_worker.purge_stale_workers(connection)
			assert_equal [0, 0, 0, 0], @limited_worker.scopes_counts(connection)
		end
	end


	def test_scopes_counts_returns_excected_value
		TestWorker.sidekiq_options(:limits => @valid_limit_options)
		create_limited_worker
		@limited_worker.update_scopes(:register)
		Sidekiq.redis do |connection|
			assert_equal [1, 1, 1, 1], @limited_worker.scopes_counts(connection)
		end
	end


	def test_scopes_keys_returns_expected_values
		host_name = 'test_host'
		Socket.expects(:gethostname).twice.returns(host_name)
		Process.expects(:pid).returns(1234)
		TestWorker.sidekiq_options(:limits => @valid_limit_options)
		create_limited_worker
		expected_values = [
			SidekiqExtensions::Limiter::LimitedWorker::PER_REDIS_KEY.to_s,
			"#{SidekiqExtensions::Limiter::LimitedWorker::PER_QUEUE_KEY}:#{@valid_message['queue']}",
			"#{SidekiqExtensions::Limiter::LimitedWorker::PER_HOST_KEY}:#{host_name}",
			"#{SidekiqExtensions::Limiter::LimitedWorker::PER_PROCESS_KEY}:#{host_name}:1234",
		].map{|key_tail| "sidekiq_extensions:limiter:limited_worker_test:test_worker:#{key_tail}"}
		assert_equal expected_values, @limited_worker.scopes_keys
	end


	def test_various_flavors_of_fetch_option
		lambda_test = lambda {|message| return message}
		TestWorker.sidekiq_options(:limits => {:test => lambda_test})
		Sidekiq.options[:limiter] = {:test => 'bar'}
		default = 'baz'
		create_limited_worker
		@limited_worker.instance_variable_set(:@message, 'foo')

		assert_equal 'foo',  @limited_worker.fetch_option(:test, default)

		TestWorker.sidekiq_options_hash['limits'][:test] = nil
		@limited_worker.instance_variable_set(:@options, nil)
		assert_equal 'bar',  @limited_worker.fetch_option(:test, default)

		Sidekiq.options[:limiter][:test] = nil
		assert_equal 'baz',  @limited_worker.fetch_option(:test, default)
	end


end
