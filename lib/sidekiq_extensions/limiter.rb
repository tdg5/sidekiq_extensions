module SidekiqExtensions

	class Limiter

		EXCEEDED_CAPACITY_MAX_RETRIES = 10

		def self.limiter_key
			key = Sidekiq.options[:namespace].to_s
			key += ':' unless key.blank?
			return key + 'sidekiq_limiter'
		end

		%w[counts locks].each do |key_name|
			self.define_method("#{key_name}_key") do |limit_type|
				return [limiter_key, limit_type, key_name].join(':')
			end


			define_method("#{key_name}_key") do |limit_type|
				return self.class.send("#{key_name}_key", limit_type)
			end
		end


		def allocate_worker
			Sidekiq.redis do |connection|
				return false unless connection.hsetnx(locks_key(:per_redis), worker_key, true)

				begin
					global_count = connection.hget(counts_key(:per_redis), worker_key)
					return false if global_count && global_count.to_i >= @redis_limit
					connection.hincrby(counts_key(:per_redis), worker_key, 1)
					return true
				ensure
					connection.hdel(locks_key(:per_redis), worker_key)
				end
			end
		end


		def call(worker, message, queue)
			@worker = worker
			puts "Worker: #{worker.inspect}\nOption: #{options}\nMessage: #{message.inspect}\nQueue: #{queue.inspect}"
			puts "Global Worker Count: #{Sidekiq.redis{|connection| connection.hget(counts_key(:per_redis), worker_key)}}"
			@redis_limit = options['per_redis']
			@host_limit = options['per_host']
			@process_limit = options['per_process']

			unless @redis_limit || @host_limit || @process_limit
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
			Sidekiq.redis{|connection| connection.hincrby(counts_key(:per_redis), worker_key, -1)}
		end


		def exceeded_capacity_retry_count
			return @message['exceeded_capacity_retry_count'] || 0
		end


		def exceeded_capacity_max_retries
			return fetch_option(:exceeded_capacity_max_retries, EXCEEDED_CAPACITY_MAX_RETRIES)
		end


		def fetch_option(option_name, default = nil)
			[options.fetch(option_name.to_s, nil), Sidekiq.options.fetch(:limiter, {}).fetch(option_name.to_sym, nil)].compact.each do |option|
				return option.call(@message) if option.respond_to?(:call)
				return option
			end
			return default
		end


		def options
			return @options ||= (@worker.class.get_sidekiq_options['limits'] || {}).stringify_keys
		end


		def retry_delay
			# By default will retry 10 times over the course of about 5 hours
			default = (exceeded_capacity_retry_count ** 4) + 15 + (rand(50) * (exceeded_capacity_retry_count + 1))
			return fetch_option(:exceeded_capacity_retry_delay, default)
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
