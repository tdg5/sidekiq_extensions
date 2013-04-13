require 'test_helper'
require 'sidekiq_extensions/priority_queue'

class PriorityQueueTest < MiniTest::Unit::TestCase

	def setup
		@test_queue = 'test'
	end


	def test_priority_queue_exists
		assert_kind_of Array, Sidekiq.options[:priority_queues]
	end


	def test_prioritize_queue_raises_an_error_unless_queue_name_is_a_string_or_symbol
		assert_raises(ArgumentError) do
			SidekiqExtensions.prioritize_queue(5)
		end
	end


	def test_prioritize_queue_raises_an_error_unless_position_argument_has_a_single_valid_key
		assert_raises(ArgumentError) do
			SidekiqExtensions.prioritize_queue(@test_queue, :before => 'before_queue', :after => 'after_queue')
		end
		assert_raises(ArgumentError) do
			SidekiqExtensions.prioritize_queue(@test_queue, :prior_to => 'before_queue')
		end
	end


	def test_prioritize_queue_raises_an_error_if_reference_queue_not_found
		Sidekiq.options[:priotiy_queues] = [@test_queue]
		assert_raises(RuntimeError) do
			SidekiqExtensions.prioritize_queue('foo', :before => 'bar')
		end
	end


	def test_prioritize_queue_raises_an_error_if_self_referential
		Sidekiq.options[:priotiy_queues] = [@test_queue]
		assert_raises(ArgumentError) do
			SidekiqExtensions.prioritize_queue('foo', :before => 'foo')
		end
	end


	def test_prioritize_queue_adds_queue_to_priority_queues_and_removes_from_queues
		symbol_test_queue = 'sym_test'
		Sidekiq.options[:queues] = [@test_queue, @test_queue, @test_queue, symbol_test_queue]
		Sidekiq.options[:priority_queues] = []

		SidekiqExtensions.prioritize_queue(@test_queue)
		refute Sidekiq.options[:queues].include?(@test_queue)
		assert_equal Sidekiq.options[:priority_queues][0], @test_queue

		SidekiqExtensions.prioritize_queue(symbol_test_queue.to_sym)
		refute Sidekiq.options[:queues].include?(symbol_test_queue)
		assert_equal Sidekiq.options[:priority_queues][0], symbol_test_queue
		assert_equal 2, Sidekiq.options[:priority_queues].length
	end


	def test_priority_queue_does_not_duplicate_values
		Sidekiq.options[:priority_queues] = []
		SidekiqExtensions.prioritize_queue(@test_queue)
		SidekiqExtensions.prioritize_queue(@test_queue)
		assert_equal 1, Sidekiq.options[:priority_queues].length
	end


	def test_prioritize_queue_adds_queue_in_correct_position
		Sidekiq.options[:priority_queues] = ['first', 'next', 'last']
		SidekiqExtensions.prioritize_queue(@test_queue)
		assert_equal Sidekiq.options[:priority_queues][0], @test_queue

		SidekiqExtensions.prioritize_queue('first', :before => @test_queue)
		assert_equal Sidekiq.options[:priority_queues][0], 'first'

		SidekiqExtensions.prioritize_queue(@test_queue, :after => :next)
		assert_equal Sidekiq.options[:priority_queues][2], @test_queue

		SidekiqExtensions.prioritize_queue(@test_queue, :after => 'last')
		assert_equal Sidekiq.options[:priority_queues][3], @test_queue

		assert_equal 4, Sidekiq.options[:priority_queues].length
	end

end
