module SidekiqExtensions

	def self.remove_queue(queue_name, options = {})
		raise ArgumentError, "Invalid queue name of type #{queue_name.class}" unless [String, Symbol].include?(queue_name.class)
		target_queue_collections = Array((options[:from] && options[:from]) || :queues).map(&:to_sym)
		target_queue_collections.each do |target_queue_collection|
			unless Sidekiq.options[target_queue_collection] && Sidekiq.options[target_queue_collection].respond_to?(:delete)
				raise ArgumentError, "Invalid queue collection #{target_queue_collection}"
			end
			Sidekiq.options[target_queue_collection].delete(queue_name.to_s)
		end
	end

end
