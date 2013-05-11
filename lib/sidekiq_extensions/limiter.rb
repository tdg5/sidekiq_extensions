require 'sidekiq_extensions/utils'
require 'redis-lock'
require 'socket'

module SidekiqExtensions

	class Limiter

		MAX_RETRIES = 10
		PER_HOST_KEY = :per_host
		PER_PROCESS_KEY = :per_process
		PER_QUEUE_KEY = :per_queue
		PER_REDIS_KEY = :per_redis
		PRIORITIZED_COUNT_SCOPES = [PER_REDIS_KEY, PER_QUEUE_KEY, PER_HOST_KEY, PER_PROCESS_KEY]


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
					update_worker_scopes(:unregister)
				end
			end

			if @worker.respond_to?(:retry)
				@worker.retry
			else
				max_retries ? try_retry : raise("Unable to allocate worker #{@worker.class.name}")
			end
		end

		protected

		def allocate_worker
			Sidekiq.redis do |connection|
				return false unless capacity_available?(connection)
				connection.lock(worker_key) do |lock|
					return false unless capacity_available?(connection)
					update_worker_scopes(:register, connection)
				end
			end
			return true
		rescue Redis::Lock::LockNotAcquired
			return false
		end


		def capacity_available?(connection, skip_purge_and_retry = false)
			availability = prioritized_limits.zip(worker_scopes_counts(connection)).map{|counts| counts.inject(:-)}.none?{|count_diff| count_diff <= 0}
			return availability if availability || skip_purge_and_retry
			purge_stale_workers(connection)
			capacity_available?(connection, true)
		end


		def fetch_option(option_name, default = nil)
			[options.fetch(option_name.to_s, nil), Sidekiq.options.fetch(:limiter, {}).fetch(option_name.to_sym, nil), default].compact.each do |option|
				return option.respond_to?(:call) ? option.call(@message) : option
			end
		end


		def limited_scopes
			return options.keys.map(&:to_sym) & PRIORITIZED_COUNT_SCOPES
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


		def purge_stale_workers(connection)
			connection.multi do
				worker_scopes_keys.each do |scope_key|
					connection.sinterstore(scope_key, scope_key, 'workers')
				end
			end
		end


		def update_worker_scopes(action, existing_connection = nil)
			adjuster = lambda do |connection|
				connection.multi do
					worker_scopes_keys.each do |worker_scope_key|
						connection.send((action == :register ? 'sadd' : 'srem'), worker_scope_key, worker_identity)
					end
				end
			end
			existing_connection ? adjuster.call(existing_connection) : Sidekiq.redis(&adjuster)
		end


		def retry_delay
			# By default will retry 10 times over the course of about 5 hours
			default = lambda{|message| (limiter_retry_count ** 4) + 15 + (rand(50) * (limiter_retry_count + 1))}
			return fetch_option(:retry_delay, default)
		end


		def schedule_retry(delay)
			Sidekiq.redis do |connection|
				connection.zadd('retry', (Time.now.to_f + delay).to_s, Sidekiq.dump_json(@message))
			end
		end


		def try_retry
			unless limiter_retry_count < max_retries
				raise "Capacity limit reached! Unable to allocate worker #{@worker.class.name}. All retries in the event of capacity limit have been exhausted."
			end

			delay = retry_delay
			@message['limiter_retry_count'] = limiter_retry_count + 1
			Sidekiq.logger.debug {"Capacity limit reached! Unable to allocate worker #{@worker.class.name}. Retry ##{limiter_retry_count} in #{delay} seconds."}
			schedule_retry(delay)
		end


		def worker_identity
			return @worker_identity ||= "#{Socket.gethostname}:#{Process.pid}-#{Thread.current.object_id}:default"
		end


		def worker_key
			return @worker_key ||= SidekiqExtensions.namespaceify(
				Sidekiq.options[:namespace],
				:sidekiq_extensions,
				:limiter,
				fetch_option(:key, @worker.class.to_s.underscore.gsub('/', ':'))
			)
		end


		def worker_scopes_counts(connection)
			current_counts = connection.multi do
				worker_scopes_keys.map{|key| connection.scard(key)}
			end
			return current_counts.map(&:to_i)
		end


		def worker_scopes_keys
			return @worker_scopes_keys ||= {
				PER_REDIS_KEY => SidekiqExtensions.namespaceify(worker_key, PER_REDIS_KEY.to_s),
				PER_QUEUE_KEY => SidekiqExtensions.namespaceify(worker_key, PER_QUEUE_KEY, @message['queue']),
				PER_HOST_KEY => SidekiqExtensions.namespaceify(worker_key, PER_HOST_KEY, Socket.gethostname),
				PER_PROCESS_KEY => SidekiqExtensions.namespaceify(worker_key, PER_PROCESS_KEY, Socket.gethostname, Process.pid),
			}.values_at(*limited_scopes)
		end

	end

end
