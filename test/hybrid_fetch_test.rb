require 'test_helper'
require 'sidekiq/fetch'
require 'sidekiq_extensions/hybrid_fetch'

class HybridQueueTest < MiniTest::Unit::TestCase

	class TestFetch < Sidekiq::BasicFetch
		include SidekiqExtensions::HybridFetch
	end


	def setup
		Sidekiq.options[:queues] = ['weighted', 'weighted', 'weighted']
		Sidekiq.options[:priority_queues] = ['high_priority', 'low_priority']
		expected_uniq_order = ['high_priority', 'low_priority', 'weighted']
		@fetch = TestFetch.new(:queues => expected_uniq_order)
		@expected_uniq_order = expected_uniq_order.map{|q| "queue:#{q}"}.concat([1])
	end


	def test_priority_queues_prioritized_before_weighted_queues
		assert_equal @expected_uniq_order, @fetch.queues_cmd
	end

end
