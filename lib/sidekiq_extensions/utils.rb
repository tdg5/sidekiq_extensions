module SidekiqExtensions

	def self.namespaceify(*components)
		return components.compact.map(&:to_s).join(':')
	end

end
