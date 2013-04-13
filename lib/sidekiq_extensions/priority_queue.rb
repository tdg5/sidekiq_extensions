require 'sidekiq_extensions/helpers'

module SidekiqExtensions

	def self.prioritize_queue(queue_name, position = {})
		raise ArgumentError, "Invalid queue name of class #{queue_name.class}" unless [String, Symbol].include?(queue_name.class)
		queue = queue_name.to_s
		if position.empty?
			remove_queue(queue, :from => :priority_queues)
			Sidekiq.options[:priority_queues].unshift(queue)
		else
			unless position.length == 1 && [:before, :after].include?(positioning = position.keys[0])
				raise ArgumentError, 'Invalid position argument. Position expects only one option with a key of :before or :after'
			end
			reference_queue = position.values[0].to_s
			raise ArgumentError, 'Queue cannot be positioned relative to itself' if reference_queue == queue
			reference_position = Sidekiq.options[:priority_queues].index(reference_queue)
			raise "Unable to find reference priority queue #{reference_queue}" unless reference_position
			existing_position = Sidekiq.options[:priority_queues].index(queue)
			reference_position -= 1 if existing_position < reference_position
			remove_queue(queue, :from => :priority_queues)
			reference_position += 1 if positioning == :after
			Sidekiq.options[:priority_queues].insert(reference_position, queue)
		end
		remove_queue(queue)
	end

end

Sidekiq.options[:priority_queues] = (Sidekiq.options[:priority_queues] || []).uniq
