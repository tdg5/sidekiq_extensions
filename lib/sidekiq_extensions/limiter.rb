require 'redis-lock'

module SidekiqExtensions

	class Limiter

		MAX_RETRIES = 10
		PER_HOST_KEY = :per_host
		PER_PROCESS_KEY = :per_process
		PER_QUEUE_KEY = :per_queue
		PER_REDIS_KEY = :per_redis
		PRIORITIZED_COUNT_SCOPES = [PER_REDIS_KEY, PER_QUEUE_KEY, PER_HOST_KEY, PER_PROCESS_KEY]


		def adjust_counts(adjustment, existing_connection = nil)
			adjuster = lambda do |connection|
				limited_scopes.each do |limited_scope|
					connection.hincrby(counts_key_for_worker, limited_scope, adjustment)
				end
			end
			existing_connection ? adjuster.call(existing_connection) : Sidekiq.redis(&adjuster)
		end


		def allocate_worker
			Sidekiq.redis do |connection|
				return false unless capacity_available?(connection)
				connection.lock(worker_key) do |lock|
					return false unless capacity_available?(connection)
					adjust_counts(1, connection)
				end
			end
			return true
		rescue Redis::Lock::LockNotAcquired
			return false
		end


		def call(worker, message, queue)
			@worker = worker
			if limited_scopes.empty?
				yield
				return
			end

			@message, @queue = message, queue

			if allocate_worker
				begin
					yield
					return
				ensure
					adjust_counts(-1)
				end
			end

			if @worker.respond_to?(:retry)
				@worker.retry
			else
				max_retries ? schedule_retry : raise("Unable to allocate worker #{@worker.class.name}")
			end
		end


		def capacity_available?(connection)
			current_counts = connection.hmget(counts_key_for_worker, limited_scopes).map(&:to_i)
			return prioritized_limits.zip(current_counts).map{|counts| counts.inject(:-)}.none?{|count_diff| count_diff <= 0}
		end


		def counts_key_for_worker
			return [limiter_key, worker_key, 'counts'].map(&:to_s).join(':')
		end


		def fetch_option(option_name, default = nil)
			[options.fetch(option_name.to_s, nil), Sidekiq.options.fetch(:limiter, {}).fetch(option_name.to_sym, nil), default].compact.each do |option|
				return option.call(@message) if option.respond_to?(:call)
				return option
			end
		end


		def limited_scopes
			return options.keys.map(&:to_sym) & PRIORITIZED_COUNT_SCOPES
		end


		def limiter_key
			return [Sidekiq.options[:namespace], :sidekiq_extensions, :limiter].compact.map(&:to_s).join(':')
		end


		def limiter_key_for_worker
			return [limiter_key, worker_key].join(':')
		end


		def limiter_retry_count
			return @message['limiter_retry_count'] || 0
		end


		def max_retries
			return fetch_option(:retry, MAX_RETRIES) || 0
		end


		def options
			return @options ||= (@worker.class.get_sidekiq_options['limits'] || {}).stringify_keys
		end


		def prioritized_limits
			return @prioritized_limits ||= limited_scopes.map{|scope| options[scope.to_s]}
		end


		def retry_delay
			# By default will retry 10 times over the course of about 5 hours
			default = lambda{|message| (limiter_retry_count ** 4) + 15 + (rand(50) * (limiter_retry_count + 1))}
			return fetch_option(:retry_delay, default)
		end


		def schedule_retry
			unless limiter_retry_count < max_retries
				raise "Capacity limit reached! Unable to allocate worker #{@worker.class.name}. All retries in the event of capacity limit have been exhausted."
			end

			delay = retry_delay
			@message['limiter_retry_count'] = limiter_retry_count + 1
			Sidekiq.logger.debug {"Capacity limit reached! Unable to allocate worker #{@worker.class.name}. Retry ##{limiter_retry_count} in #{delay} seconds."}

			Sidekiq.redis do |connection|
				connection.zadd('retry', (Time.now.to_f + delay).to_s, Sidekiq.dump_json(@message))
			end
		end


		def worker_key
			@worker_key ||= fetch_option(:key, @worker.class.to_s.underscore.gsub('/', ':'))
		end

	end

end
