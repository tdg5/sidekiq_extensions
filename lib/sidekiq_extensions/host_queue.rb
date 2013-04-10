require 'socket'

module Sidekiq

	def self.host_queue
		return options[:host_queue] || Socket.gethostname
	end

end

Sidekiq.options[:queues] << config.host_queue unless config.options[:queues].include?(config.host_queue)
