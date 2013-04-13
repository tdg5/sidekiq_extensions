require 'socket'

module SidekiqExtensions

	def self.host_queue
		return Sidekiq.options[:host_queue] || Socket.gethostname
	end


	def self.register_host_queue(weight = 1)
		weight = 1 unless weight.is_a?(Fixnum) && weight > 0
		existing_weight = Sidekiq.options[:queues].select{|queue| queue == host_queue}.length
		needed_weight = weight - existing_weight
		return unless needed_weight > 0
		needed_weight.times do
			Sidekiq.options[:queues] << host_queue
		end
	end

end

