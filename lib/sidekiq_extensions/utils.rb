module SidekiqExtensions

	def self.namespaceify(*components)
		return components.compact.map(&:to_s).join(':')
	end


	def self.thread_identity
		return "#{Socket.gethostname}:#{Process.pid}-#{Thread.current.object_id}:default"
	end

end
