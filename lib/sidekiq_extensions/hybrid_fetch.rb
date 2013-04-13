require 'sidekiq_extensions/priority_queue'

module SidekiqExtensions

	module HybridFetch

		def initialize(options)
			@strictly_ordered_queues = !!Sidekiq.options[:strict]
			@queues = Sidekiq.options[:queues]
			@priority_queues = Sidekiq.options[:priority_queues]
			[@queues, @priority_queues].each{|queues| queues.map!{|q| "queue:#{q}"}}
			@unique_queues = (@priority_queues + @queues).uniq
		end


		def queues_cmd
			queues = @strictly_ordered_queues ? @unique_queues.dup : (@priority_queues + @queues.shuffle).uniq
			queues << Sidekiq::Fetcher::TIMEOUT
		end

	end

end
