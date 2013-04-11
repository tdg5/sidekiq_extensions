lib = File.expand_path('../lib', __FILE__)
$:.push(lib) unless $:.include?(lib)
require 'sidekiq_extensions/version'

Gem::Specification.new do |gem|
	gem.authors = ['Freewrite.org']
	gem.description = 'Extensions for Sidekiq message processor'
	gem.email = ['freewrite.org@gmail.com']
	gem.homepage = 'http://github.com/freewrite/sidekiq_extensions'
	gem.name = 'sidekiq_extensions'
	gem.summary = 'Extensions for Sidekiq message processor'
	gem.version = SidekiqExtensions::VERSION

	gem.files = `git ls-files`.split("\n")
	gem.require_paths = %w[lib]
	gem.test_files = Dir['test/*']

	gem.add_dependency 'sidekiq', '2.8.0'
	gem.add_development_dependency 'rake'
end
