require 'redis-lock'
require 'socket'

module SidekiqExtensions

	class Limiter

		MAX_RETRIES = 10

		class CapacityLimitError < StandardError; end


		def call(worker, message, queue)
			@limited_worker = LimitedWorker.new(worker, message)
			if @limited_worker.limited_scopes.empty?
				yield
				return
			end

			@message, @queue = message, queue

			allocate_worker
			begin
				yield
			ensure
				@limited_worker.update_scopes(:unregister)
			end

		rescue CapacityLimitError
			if worker.respond_to?(:retry)
				worker.retry
			else
				@limited_worker.max_retries ? try_retry : raise("Unable to allocate worker #{worker.class.name}")
			end
		end

		protected

		def allocate_worker
			Sidekiq.redis do |connection|
				raise CapacityLimitError unless @limited_worker.capacity_available?(connection)
				connection.lock(@limited_worker.key) do |lock|
					raise CapacityLimitError unless @limited_worker.capacity_available?(connection)
					@limited_worker.update_scopes(:register, connection)
				end
			end
		rescue Redis::Lock::LockNotAcquired
			raise CapacityLimitError
		end


		def limiter_retry_count
			return @message['limiter_retry_count'] || 0
		end


		def schedule_retry(delay)
			Sidekiq.redis do |connection|
				connection.zadd('retry', (Time.now.to_f + delay).to_s, Sidekiq.dump_json(@message))
			end
		end


		def try_retry
			unless limiter_retry_count < @limited_worker.max_retries
				raise "Capacity limit reached! Unable to allocate worker #{@limited_worker.worker.class.name}. All retries in the event of capacity limit have been exhausted."
			end

			delay = @limited_worker.retry_delay(limiter_retry_count)
			@message['limiter_retry_count'] = limiter_retry_count + 1
			Sidekiq.logger.debug {"Capacity limit reached! Unable to allocate worker #{@limited_worker.worker.class.name}. Retry ##{limiter_retry_count} in #{delay} seconds."}
			schedule_retry(delay)
		end

	end

end
