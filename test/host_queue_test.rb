require 'test_helper'
require 'sidekiq_extensions/host_queue'

class HostQueueTest < MiniTest::Unit::TestCase

	def test_host_queue_method_on_sidekiq_module
		explicit_name = 'explicit_queue_name'
		Socket.stubs(:gethostname).returns('test_host')

		Sidekiq.options[:host_queue] = explicit_name
		assert_equal explicit_name, Sidekiq.host_queue

		Sidekiq.options[:host_queue] = nil
		assert_equal 'test_host', Sidekiq.host_queue
	end


	def test_host_name_queue_added_to_server_queues
		Socket.stubs(:gethostname).returns('test_host')
		Sidekiq.options[:queues] = []
		Sidekiq.register_host_queue
		assert_equal ['test_host'], Sidekiq.options[:queues]
	end


	def test_host_queue_only_added_in_needed_weight
		Socket.stubs(:gethostname).returns('test_host')
		Sidekiq.options[:queues] = []
		3.times {Sidekiq.options[:queues] << Sidekiq.host_queue}

		Sidekiq.register_host_queue(6)
		assert_equal 6, Sidekiq.options[:queues].select{|queue| queue == Sidekiq.host_queue}.length
	end

end
