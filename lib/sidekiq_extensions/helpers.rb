module SidekiqExtensions

	def self.remove_queue(queue_name, options = {})
		raise ArgumentError, "Invalid queue name of type #{queue_name.class}" unless [String, Symbol].include?(queue_name.class)
		target_queue_collection = (options[:from] && options[:from].to_sym) || :queues
		raise ArgumentError, "Invalid queue collection #{target_queue_collection}" unless Sidekiq.options[target_queue_collection]
		Sidekiq.options[target_queue_collection].delete(queue_name.to_s)
	end

end
