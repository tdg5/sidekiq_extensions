module SidekiqExtensions

	module PriorityFetch

		def initialize(options)
			@strictly_ordered_queues = !!options[:strict]
			@queues = options[:queues]
			@priority_queues = options[:priority_queues]
			[@queues, @priority_queues].map{|queues| queues.map {|q| "queue:#{q}"}}
			@unique_queues = (@priority_queues + @queues).uniq
		end


		def queues_cmd
			queues = @strictly_ordered_queues ? @unique_queues.dup : (@priority_queues + @queues.shuffle).uniq
			queues << Sidekiq::Fetcher::TIMEOUT
		end
	end

end

Sidekiq.options[:priority_queues] ||= []
