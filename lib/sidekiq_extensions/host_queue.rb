require 'socket'

module Sidekiq

	def self.host_queue
		return options[:host_queue] || Socket.gethostname
	end

end

Sidekiq.options[:queues] << Sidekiq.host_queue unless Sidekiq.options[:queues].include?(Sidekiq.host_queue)
