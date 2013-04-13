require 'test_helper'
require 'sidekiq_extensions/host_queue'

class HostQueueTest < MiniTest::Unit::TestCase

	def setup
		@host_name = 'test_host'
		Socket.stubs(:gethostname).returns(@host_name)
	end

	def test_host_queue_method_on_sidekiq_module
		explicit_name = 'explicit_queue_name'

		Sidekiq.options[:host_queue] = explicit_name
		assert_equal explicit_name, SidekiqExtensions.host_queue

		Sidekiq.options[:host_queue] = nil
		assert_equal @host_name, SidekiqExtensions.host_queue
	end


	def test_host_name_queue_added_to_server_queues
		Sidekiq.options[:queues] = []
		SidekiqExtensions.register_host_queue
		assert_equal [@host_name], Sidekiq.options[:queues]
	end


	def test_host_queue_only_added_in_needed_weight
		Sidekiq.options[:queues] = []
		3.times {Sidekiq.options[:queues] << SidekiqExtensions.host_queue}

		SidekiqExtensions.register_host_queue(6)
		assert_equal 6, Sidekiq.options[:queues].select{|queue| queue == SidekiqExtensions.host_queue}.length
	end

end
