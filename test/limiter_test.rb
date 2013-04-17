require 'test_helper'
require 'sidekiq_extensions/limiter'

class LimiterTest < MiniTest::Unit::TestCase

	class TestWorker
		include Sidekiq::Worker
	end


	def setup
		@limiter = SidekiqExtensions::Limiter.new
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


	def test_allocate_worker_returns_false_if_any_limit_at_capacity
		TestWorker.sidekiq_options(:limits => @valid_limit_options)
		@limiter.instance_variable_set(:@worker, TestWorker.new)
		@limiter.instance_variable_set(:@message, @valid_message)
		Redis::Namespace.any_instance.expects(:hmget).once.returns([94, 93, 92, 91])
		refute @limiter.allocate_worker
	end


	def test_allocate_worker_returns_false_if_lock_cannot_be_obtained
		TestWorker.sidekiq_options(:limits => @valid_limit_options)
		@limiter.instance_variable_set(:@worker, TestWorker.new)
		@limiter.instance_variable_set(:@message, @valid_message)
		Redis::Namespace.any_instance.expects(:lock).once.raises(Redis::Lock::LockNotAcquired)
		refute @limiter.allocate_worker
	end


	def test_allocate_worker_returns_false_if_no_capacity_available_after_locking
		TestWorker.sidekiq_options(:limits => @valid_limit_options)
		@limiter.instance_variable_set(:@worker, TestWorker.new)
		@limiter.expects(:capacity_available?).twice.returns(true).then.returns(false)
		refute @limiter.allocate_worker
	end


	def test_allocate_worker_returns_true_if_no_limit_met
		TestWorker.sidekiq_options(:limits => @valid_limit_options)
		@limiter.instance_variable_set(:@worker, TestWorker.new)
		@limiter.instance_variable_set(:@message, @valid_message)
		Redis::Namespace.any_instance.expects(:hincrby).times(SidekiqExtensions::Limiter::PRIORITIZED_COUNT_SCOPES.count)
		assert @limiter.allocate_worker
	end


	def test_allocate_worker_returns_true_regardless_of_jobs_that_do_not_relate_to_current_worker_context
		TestWorker.sidekiq_options(:limits => @valid_limit_options)
		@limiter.instance_variable_set(:@worker, TestWorker.new)
		@limiter.instance_variable_set(:@message, @valid_message)
		Sidekiq.redis do |connection|
			@limiter.worker_scopes_keys.each do |worker_scope_key|
				connection.hincrby(@limiter.counts_key_for_worker, "unrelated:#{worker_scope_key}", 4)
			end
		end
		Redis::Namespace.any_instance.expects(:hincrby).times(SidekiqExtensions::Limiter::PRIORITIZED_COUNT_SCOPES.count)
		assert @limiter.allocate_worker
	end


	def test_counts_key_for_worker_returns_expected_values
		@limiter.instance_variable_set(:@worker, TestWorker.new)
		assert_equal 'sidekiq_extensions:limiter:limiter_test:test_worker:counts', @limiter.counts_key_for_worker
	end


	def test_counts_key_for_worker_uses_custom_worker_key
		TestWorker.sidekiq_options(:limits => {:key => 'test_worker_key'})
		@limiter.instance_variable_set(:@worker, TestWorker.new)
		assert_equal 'sidekiq_extensions:limiter:test_worker_key:counts', @limiter.counts_key_for_worker
	end


	def test_key_for_scope_returns_expected_values
		host_name = 'test_host'
		Socket.expects(:gethostname).twice.returns(host_name)
		Process.expects(:pid).returns(1234)
		@limiter.instance_variable_set(:@message, @valid_message)
		expected_values = {
			SidekiqExtensions::Limiter::PER_HOST_KEY => "#{SidekiqExtensions::Limiter::PER_HOST_KEY}:#{host_name}",
			SidekiqExtensions::Limiter::PER_PROCESS_KEY => "#{SidekiqExtensions::Limiter::PER_PROCESS_KEY}:#{host_name}:1234",
			SidekiqExtensions::Limiter::PER_QUEUE_KEY => "#{SidekiqExtensions::Limiter::PER_QUEUE_KEY}:#{@valid_message['queue']}",
			SidekiqExtensions::Limiter::PER_REDIS_KEY => SidekiqExtensions::Limiter::PER_REDIS_KEY,
		}
		SidekiqExtensions::Limiter::PRIORITIZED_COUNT_SCOPES.each do |scope|
			assert_equal expected_values[scope], @limiter.key_for_scope(scope)
		end
	end


	def test_limited_scopes_returns_expected_values
		limits = Hash[SidekiqExtensions::Limiter::PRIORITIZED_COUNT_SCOPES.map{|scope| [scope, 1]}]
		scopes = limits.keys
		TestWorker.sidekiq_options(:limits => limits)
		@limiter.instance_variable_set(:@worker, TestWorker.new)
		until limits.empty?
			@limiter.instance_variable_set(:@options, nil)
			assert_equal scopes, @limiter.limited_scopes
			TestWorker.sidekiq_options_hash['limits'].delete(scopes.delete(scopes.sample))
		end
	end


	def test_limiter_decrements_counts_even_on_failure
		expected = 1
		Redis::Namespace.any_instance.expects(:hmget).times(4).returns([0] * 4)
		@limiter.expects(:adjust_counts).times(4).with do |adjustment, connection|
			(adjustment == expected) && (expected *= -1)
		end
		TestWorker.sidekiq_options(:limits => @valid_limit_options)
		@limiter.call(TestWorker.new, @valid_message, 'test') {}
		assert_raises(RuntimeError) do
			@limiter.call(TestWorker.new, @valid_message, 'test') do
				raise 'He\'s dead, Jim'
			end
		end
	end


	def test_limiter_defers_to_worker_retry_method_if_available
		@limiter.expects(:allocate_worker).returns(false)
		TestWorker.sidekiq_options(:limits => @valid_limit_options)
		worker = TestWorker.new
		worker.expects(:respond_to?).with(:retry).returns(true)
		worker.expects(:retry)
		@limiter.expects(:schedule_retry).never
		@limiter.call(worker, @valid_message, 'test') {}
	end


	def test_limiter_key_returns_expected_keys
		assert_equal 'sidekiq_extensions:limiter', @limiter.limiter_key
		Sidekiq.options[:namespace] = 'test'
		assert_equal 'test:sidekiq_extensions:limiter', @limiter.limiter_key
		Sidekiq.options[:namespace] = nil
	end


	def test_limiter_yields_immediately_with_no_limited_scopes
		TestWorker.sidekiq_options(:limits => {})
		@limiter.expects(:allocate_worker).never
		@limiter.call(TestWorker.new, {}, 'test') {}
	end


	def test_redis_lock_is_configured_correctly
		Sidekiq.redis do |connection|
			assert connection.respond_to?(:lock)
		end
	end


	def test_retry_raises_error_if_retry_disabled
		@limiter.expects(:allocate_worker).returns(false)
		@valid_limit_options[:retry] = false
		TestWorker.sidekiq_options(:limits => @valid_limit_options)
		assert_raises(RuntimeError) do
			@limiter.call(TestWorker.new, @valid_message, 'test') {}
		end
	end


	def test_retry_scheduled_if_unable_to_allocate_worker
		@limiter.expects(:allocate_worker).returns(false)
		@limiter.expects(:schedule_retry).once
		TestWorker.sidekiq_options(:limits => @valid_limit_options)
		@limiter.call(TestWorker.new, @valid_message, 'test') {}
	end


	def test_retry_scheduling_increments_limiter_retry_count_and_sends_to_redis
		@limiter.expects(:allocate_worker).returns(false)
		TestWorker.sidekiq_options(:limits => @valid_limit_options)
		@valid_message['limiter_retry_count'] = 1
		Redis::Namespace.any_instance.expects(:zadd).once
		Sidekiq.expects(:dump_json).with(@valid_message)
		@limiter.call(TestWorker.new, @valid_message, 'test') {}
		assert_equal 2, @limiter.limiter_retry_count
	end


	def test_retry_scheduling_raises_error_if_no_more_retries_left
		@limiter.expects(:allocate_worker).returns(false)
		@valid_limit_options[:retry] = 0
		TestWorker.sidekiq_options(:limits => @valid_limit_options)
		assert_raises(RuntimeError) do
			@limiter.call(TestWorker.new, @valid_message, 'test') {}
		end
	end


	def test_various_flavors_of_fetch_option
		lambda_test = lambda {|message| return message}
		TestWorker.sidekiq_options(:limits => {:test => lambda_test})
		Sidekiq.options[:limiter] = {:test => 'bar'}
		default = 'baz'
		@limiter.instance_variable_set(:@worker, worker = TestWorker.new)
		@limiter.instance_variable_set(:@message, 'foo')

		assert_equal 'foo',  @limiter.fetch_option(:test, default)

		worker.sidekiq_options_hash['limits'][:test] = nil
		@limiter.instance_variable_set(:@options, nil)
		assert_equal 'bar',  @limiter.fetch_option(:test, default)

		Sidekiq.options[:limiter][:test] = nil
		assert_equal 'baz',  @limiter.fetch_option(:test, default)
	end

end
