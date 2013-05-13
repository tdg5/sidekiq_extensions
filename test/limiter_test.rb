require 'test_helper'
require 'sidekiq_extensions/limiter'

class LimiterTest < MiniTest::Unit::TestCase

	class TestWorker
		include Sidekiq::Worker
	end


	def set_limited_worker(worker_class = TestWorker)
		@limiter.instance_variable_set(:@limited_worker, SidekiqExtensions::Limiter::LimitedWorker.new(worker_class.new, @valid_message))
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


	def test_allocate_worker_raise_error_if_lock_cannot_be_obtained
		TestWorker.sidekiq_options(:limits => @valid_limit_options)
		set_limited_worker
		@limiter.instance_variable_set(:@message, @valid_message)
		Redis::Namespace.any_instance.expects(:lock).once.raises(Redis::Lock::LockNotAcquired)
		assert_raises(SidekiqExtensions::Limiter::CapacityLimitError) do
			@limiter.send(:allocate_worker)
		end
	end


	def test_allocate_worker_raises_error_if_no_capacity_available_after_locking
		TestWorker.sidekiq_options(:limits => @valid_limit_options)
		set_limited_worker
		SidekiqExtensions::Limiter::LimitedWorker.any_instance.expects(:capacity_available?).twice.returns(true).then.returns(false)
		assert_raises(SidekiqExtensions::Limiter::CapacityLimitError) do
			@limiter.send(:allocate_worker)
		end
	end


	def test_allocate_worker_returns_nil_if_no_limit_met
		TestWorker.sidekiq_options(:limits => @valid_limit_options)
		set_limited_worker
		@limiter.instance_variable_set(:@message, @valid_message)
		SidekiqExtensions::Limiter::LimitedWorker.any_instance.expects(:update_scopes).once
		assert_nil @limiter.send(:allocate_worker)
	end


	def test_limiter_defers_to_worker_retry_method_if_available
		@limiter.expects(:allocate_worker).raises(SidekiqExtensions::Limiter::CapacityLimitError)
		TestWorker.sidekiq_options(:limits => @valid_limit_options)
		worker = TestWorker.new
		worker.expects(:respond_to?).with(:retry).returns(true)
		worker.expects(:retry)
		@limiter.expects(:schedule_retry).never
		@limiter.call(worker, @valid_message, 'test') {}
	end


	def test_limiter_retry_raises_error_if_retry_disabled
		@limiter.expects(:allocate_worker).raises(SidekiqExtensions::Limiter::CapacityLimitError)
		@valid_limit_options[:retry] = false
		TestWorker.sidekiq_options(:limits => @valid_limit_options)
		assert_raises(RuntimeError) do
			@limiter.call(TestWorker.new, @valid_message, 'test') {}
		end
	end


	def test_limiter_retry_scheduling_increments_limiter_retry_count_and_sends_to_redis
		@limiter.expects(:allocate_worker).raises(SidekiqExtensions::Limiter::CapacityLimitError)
		TestWorker.sidekiq_options(:limits => @valid_limit_options)
		@valid_message['limiter_retry_count'] = 1
		Redis::Namespace.any_instance.expects(:zadd).once
		Sidekiq.expects(:dump_json).with(@valid_message)
		@limiter.call(TestWorker.new, @valid_message, 'test') {}
		assert_equal 2, @limiter.send(:limiter_retry_count)
	end


	def test_limiter_retry_scheduling_raises_error_if_no_more_retries_left
		@limiter.expects(:allocate_worker).raises(SidekiqExtensions::Limiter::CapacityLimitError)
		@valid_limit_options[:retry] = 0
		TestWorker.sidekiq_options(:limits => @valid_limit_options)
		assert_raises(RuntimeError) do
			@limiter.call(TestWorker.new, @valid_message, 'test') {}
		end
	end


	def test_limited_schedules_retry_if_unable_to_allocate_worker
		@limiter.expects(:allocate_worker).raises(SidekiqExtensions::Limiter::CapacityLimitError)
		@limiter.expects(:schedule_retry).once
		TestWorker.sidekiq_options(:limits => @valid_limit_options)
		@limiter.call(TestWorker.new, @valid_message, 'test') {}
	end


	def test_limiter_unregisters_worker_even_on_failure
		expected = :register
		SidekiqExtensions::Limiter::LimitedWorker.any_instance.expects(:scopes_counts).times(4).returns([0] * 4)
		SidekiqExtensions::Limiter::LimitedWorker.any_instance.expects(:update_scopes).times(4).with do |adjustment, connection|
			(adjustment == expected) && (expected = (expected == :register ? :unregister : :register))
		end
		TestWorker.sidekiq_options(:limits => @valid_limit_options)
		@limiter.call(TestWorker.new, @valid_message, 'test') {}
		assert_raises(RuntimeError) do
			@limiter.call(TestWorker.new, @valid_message, 'test') do
				raise 'He\'s dead, Jim'
			end
		end
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

end
