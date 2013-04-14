require 'rake/testtask'
require 'bundler/gem_tasks'

Rake.application.options.suppress_backtrace_pattern = /.*/
Rake::TestTask.new do |t|
	t.libs << 'test'
	t.pattern = 'test/*_test.rb'
end

task :default => :test
