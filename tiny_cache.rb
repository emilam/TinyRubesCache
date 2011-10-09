require 'rubygems'
require 'eventmachine'
require 'singleton'
require 'yaml'

class TinyCache
	include Singleton

	def initialize
		@cache_container = {}
	end

	def set(key, value, ttl)
		@cache_container[key] = {:value => value, :ttl => ttl, :max_ttl => ttl}
		"STORED"
	end

	def add(key, value, ttl)
		unless @cache_container.include? key
			@cache_container[key] = {:value => value, :ttl => ttl, :max_ttl => ttl}
			"STORED"
		else
			"NOT STORED"
		end
	end

	def replace(key, value, ttl)
		if @cache_container.include? key
			@cache_container[key] = {:value => value, :ttl => ttl, :max_ttl => ttl}
			"STORED"
		else
			"NOT STORED"
		end
	end

	def get_value(key)
		entry = @cache_container[key]
		entry[:ttl] = entry[:max_ttl]
		value = entry[:value]
		"VALUE #{key} 0 #{value.length}\r\n#{value}"
	end

	def get(key)
		if @cache_container.include? key
			self.get_value(key) << "\r\nEND"
		else
			"NOT FOUND"
		end
	end

	def gets(keys)
		gets_lines = keys.map {
			|key|
			if @cache_container.include? key
				self.get_value(key)
			end
		}.join("\r\n")
		puts gets_lines
		gets_lines << "\r\nEND"
	end

	def remove_expired(time_since=1)
		@cache_container.delete_if {
			|key, value|
			value[:ttl] -= time_since
			value[:ttl] <= 0
		}
	end
end

class TinyCacheManager

	def initialize
		@storage_index = {:cmd => 0, :key => 1, :flags => 2, :ttl => 3, :length => 4}
		@retrieval_index = {:cmd => 0, :key => 1}
		@state = :command
		@storage = nil
	end

	def version(*args)
		"VERSION 0.1 TinyCache"
	end

	def gets(tokens)
		TinyCache.instance.gets(tokens[@retrieval_index[:key]..-1])
	end

	def get(tokens)
		TinyCache.instance.get(tokens[@retrieval_index[:key]])
	end

	def set(tokens)
		self.enter_store_state(tokens, 'set')
	end

	def replace(tokens)
		self.enter_store_state(tokens, 'replace')
	end

	def add(tokens)
		self.enter_store_state(tokens, 'add')
	end

	def handle_command(command)
		tokens = command.split
		command_name = tokens[@retrieval_index[:cmd]].downcase
		if self.respond_to? command_name
			return self.method(command_name).call tokens
		else
			return "ERROR"
		end
	end

	def handle_store(data)
		self.store_data(@storage[:tokens], @storage[:data], data)
	end

	def parse(command)
		if @state == :command
			self.handle_command(command)
		elsif @state == :store
			self.handle_store(command)
		else
			"UNKNOWN STATE #{@state}"
		end
	end

	def enter_store_state(tokens, method)
		@state = :store
		@storage = {:tokens => tokens, :data => '', :method => method}
	end

	def store_data(tokens, existing_data, data)
		existing_data = existing_data << data
		@storage[:data] = existing_data
		length = tokens[@storage_index[:length]].to_i
		if existing_data.length > length
			value = existing_data[0..-3]
			key = tokens[@storage_index[:key]]
			ttl = tokens[@storage_index[:ttl]].to_i
			store_method = TinyCache.instance.method(@storage[:method])
			response = store_method.call(key, value, ttl)
			@storage = nil
			@state = :command
			response
		end
	end

end

class TinyCacheServer < EM::Connection
	def initialize(*args)
		super
		@manager = TinyCacheManager.new
		puts "TinyCacheServer started."
	end
	def receive_data(data)
		response = @manager.parse(data)
		send_data(response << "\r\n") if response.respond_to? :concat
	end
end

class TinyCacheConfig
	attr_reader :port
	attr_reader :host
	attr_reader :sweep_interval
	def initialize
		conf_file = "tiny_cache.conf"
		puts "Loading #{conf_file}"
		config = YAML::load(File.open(conf_file))
		@port = config['port']
		@host = config['host']
		@sweep_interval = config['sweep_interval']
		config
	end
end

EM.run do
	cache_config = TinyCacheConfig.new
	puts "Starting server on #{cache_config.host}:#{cache_config.port}"
	EM.start_server(cache_config.host, cache_config.port, TinyCacheServer)
	EventMachine::PeriodicTimer.new(cache_config.sweep_interval) do
		  TinyCache.instance.remove_expired cache_config.sweep_interval
	end
end
