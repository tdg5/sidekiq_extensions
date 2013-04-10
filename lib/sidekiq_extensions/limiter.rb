module SidekiqExtensions

	class Limiter

		EXCEEDED_CAPACITY_MAX_RETRIES = 10

		def self.global_counts_key
			return global_limiter_key + ':counts'
		end

		def self.global_limiter_key
			key = Sidekiq.options[:namespace].to_s
			key += ':' unless key.blank?
			return key + 'sidekiq_limiter'
		end


		def self.global_locks_key
			return global_limiter_key + ':locks'
		end


		def allocate_worker
			Sidekiq.redis do |connection|
				return false unless connection.hsetnx(global_locks_key, worker_key, true)

				begin
					global_count = connection.hget(global_counts_key, worker_key)
					return false if global_count && global_count.to_i >= @global_limit
					connection.hincrby(global_counts_key, worker_key, 1)
					return true
				ensure
					connection.hdel(global_locks_key, worker_key)
				end
			end
		end


		def call(worker, message, queue)
			@worker = worker
			puts "Worker: #{worker.inspect}\nOption: #{options}\nMessage: #{message.inspect}\nQueue: #{queue.inspect}"
			puts "Global Worker Count: #{Sidekiq.redis{|connection| connection.hget(global_counts_key, worker_key)}}"
			@global_limit = options['global']

			if @global_limit.nil?
				yield
				return
			end
			@message, @queue = message, queue

			if allocate_worker
				begin
					yield
				ensure
					decrement
				end
			else
				if @worker.respond_to?(:exceeded_capacity_strategy)
					@worker.exceeded_capacity_strategy
				else
					schedule_retry
				end
			end
		end


		def decrement
			Sidekiq.redis{|connection| connection.hincrby(global_counts_key, worker_key, -1)}
		end


		def exceeded_capacity_retry_count
			return @message['exceeded_capacity_retry_count'] || 0
		end


		%w[global_counts_key global_locks_key].each do |key_method|
			define_method(key_method) do
				return self.class.send(key_method)
			end
		end


		def exceeded_capacity_max_retries
			[options.fetch('exceeded_capacity_max_retries', nil), Sidekiq.options.fetch(:limiter, {}).fetch(:exceeded_capacity_max_retries, nil)].compact.each do |option|
				return option.call(@message) if option.respond_to?(:call)
				return option
			end
			return EXCEEDED_CAPACITY_MAX_RETRIES
		end


		def options
			return @options ||= (@worker.class.get_sidekiq_options['limits'] || {}).stringify_keys
		end


		def retry_delay
			[options.fetch('exceeded_capacity_retry_delay', nil), Sidekiq.options.fetch(:limiter, {}).fetch(:exceeded_capacity_retry_delay, nil)].compact.each do |option|
				return option.call(@message) if option.respond_to?(:call)
				return option
			end

			# By default will retry 10 times over the course of about 5 hours
			return (exceeded_capacity_retry_count ** 4) + 15 + (rand(50) * (exceeded_capacity_retry_count + 1))
		end


		def schedule_retry
			unless exceeded_capacity_retry_count < exceeded_capacity_max_retries
				raise "Capacity exceeded! Unable to allocate worker #{@worker.class.name}. All retries in the event of exceeded capacity have been exhausted."
			end

			delay = retry_delay
			@message['exceeded_capacity_retry_count'] = exceeded_capacity_retry_count + 1
			Sidekiq.logger.debug {"Capacity exceeded! Unable to allocate worker #{@worker.class.name}. Retry ##{exceeded_capacity_retry_count} in #{delay} seconds."}

			Sidekiq.redis do |connection|
				connection.zadd('retry', (Time.now.to_f + delay).to_s, Sidekiq.dump_json(@message))
			end
		end


		def worker_key
			return @worker_key ||= (options['key'] || @worker.class.to_s.underscore.gsub('/', ':'))
		end

	end

end
