require 'test_helper'
require 'sidekiq_extensions/helpers'

class HelpersTest < MiniTest::Unit::TestCase

	def setup
		@test_queue = 'test'
	end


	def test_remove_queue_raises_error_unless_queue_collection_exists
		assert_raises(ArgumentError) do
			SidekiqExtensions.remove_queue('test', :from => 'mia_queue')
		end
	end


	def test_remove_queue_raises_error_unless_queue_name_is_string_or_symbol
		assert_raises(ArgumentError) do
			SidekiqExtensions.remove_queue(5)
		end
	end


	def test_remove_queue_removes_queue_from_queues_by_default
		Sidekiq.options[:queues] = [@test_queue]
		assert_equal Sidekiq.options[:queues][0], @test_queue
		SidekiqExtensions.remove_queue(@test_queue)
		assert Sidekiq.options[:queues].empty?, 'Queue was not successfully removed'
	end


	def test_remove_queue_removes_all_instances_of_queue
		Sidekiq.options[:queues] = [@test_queue, @test_queue, @test_queue]
		assert_equal 3, Sidekiq.options[:queues].length
		SidekiqExtensions.remove_queue(@test_queue)
		assert Sidekiq.options[:queues].empty?, 'All instances of queue were not successfully removed'
	end


	def test_remove_queue_can_target_other_queue_collections
		require 'sidekiq_extensions/priority_queue'

		assert_kind_of Array, Sidekiq.options[:priority_queues]
		SidekiqExtensions.prioritize_queue(@test_queue)
		assert_equal @test_queue, Sidekiq.options[:priority_queues][0]
		SidekiqExtensions.remove_queue(@test_queue, :from => :priority_queues)
		assert Sidekiq.options[:priority_queues].empty?, 'Queue was not successfully removed from target queue collection'
	end

end
