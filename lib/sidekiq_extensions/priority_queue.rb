require 'sidekiq_extensions/helpers'

module SidekiqExtensions

	def self.prioritize_queue(queue_name, position = {})
		raise ArgumentError, "Invalid queue name of class #{queue_name.class}" unless [String, Symbol].include?(queue_name.class)
		unless position.empty? || position.length == 1 && [:after, :before].include?(position.keys[0])
			raise ArgumentError, 'Invalid position argument. Position expects only one option with a key of :before or :after'
		end
		insert_at = insertion_index(queue_name.to_s, position.keys[0], position.values[0].to_s)
		remove_queue(queue_name, :from => [:priority_queues, :queues])
		Sidekiq.options[:priority_queues].insert(insert_at, queue_name.to_s)
	end


	private
	def self.insertion_index(queue, position = nil, reference_queue = nil)
		return 0 if position.nil? || reference_queue.empty?
		raise ArgumentError, 'Queue cannot be positioned relative to itself' if reference_queue == queue
		reference_position = Sidekiq.options[:priority_queues].index(reference_queue)
		raise "Unable to find reference priority queue #{reference_queue}" unless reference_position
		existing_position = Sidekiq.options[:priority_queues].index(queue)
		reference_position -= 1 if existing_position && existing_position < reference_position
		return position == :after ? reference_position + 1 : reference_position
	end

end

Sidekiq.options[:priority_queues] = (Sidekiq.options[:priority_queues] || []).uniq
