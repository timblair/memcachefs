#!/usr/bin/env ruby
require 'rubygems'
require 'eventmachine'
require 'socket'
require 'fileutils'
require 'digest/md5'
require 'sdbm'

module MemcacheFS

  class Server
    DEFAULT_HOST    = '127.0.0.1'
    DEFAULT_PORT    = 33133
    DEFAULT_PATH    = "/tmp/memcachefs"
    DEFAULT_FILE    = "memcachefs.db"
    DEFAULT_TIMEOUT = 60

    def self.start(opts = {})
      server = self.new(opts)
      server.start
    end

    def initialize(opts = {})
      @@instance = self
      @opts = {
        :host    => DEFAULT_HOST,
        :port    => DEFAULT_PORT,
        :path    => DEFAULT_PATH,
        :file    => DEFAULT_FILE,
        :timeout => DEFAULT_TIMEOUT,
        :server  => self
      }.merge(opts)
      FileUtils.mkdir_p(@opts[:path])
      @db = SDBM.new File.join(@opts[:path], @opts[:file])
    end

    def start
      EventMachine.epoll
      EventMachine.set_descriptor_table_size(4096)
      EventMachine.run do
        EventMachine.start_server(@opts[:host], @opts[:port], MemcacheFS::ConnectionHandler, { :opts => @opts, :db => @db })
        puts "Started MemcacheFS on #{@opts[:host]}:#{@opts[:port]}..."
      end
    end

    def self.shutdown
      @@instance.stop
    end

    def stop
      EventMachine.stop_event_loop
    end
  end # class Server

  class ConnectionHandler < EventMachine::Connection

    # command pragmas
    GET_COMMAND           = /\Aget (.{1,250})\s*\r\n/m
    SET_COMMAND           = /\Aset (.{1,250}) ([0-9]+) ([0-9]+) ([0-9]+)\r\n/m
    DELETE_COMMAND        = /\Adelete (.{1,250}) ([0-9]+)\r\n/m
    SHUTDOWN_COMMAND      = /\Ashutdown\r\n/m
    QUIT_COMMAND          = /\Aquit\r\n/m
    FLUSH_ALL_COMMAND     = /\Aflush_all\r\n/m
    # response pragmas
    ERR_UNKNOWN_COMMAND   = "CLIENT_ERROR bad command line format\r\n".freeze
    GET_RESPONSE          = "VALUE %s %s %s\r\n%s\r\nEND\r\n".freeze
    GET_RESPONSE_EMPTY    = "END\r\n".freeze
    SET_RESPONSE_SUCCESS  = "STORED\r\n".freeze
    SET_RESPONSE_FAILURE  = "NOT STORED\r\n".freeze
    DELETE_RESPONSE       = "END\r\n".freeze
    NOT_FOUND_RESPONSE    = "NOT_FOUND\r\n".freeze
    OK_RESPONSE           = "OK\r\n".freeze

    @@next_session_id = 1

    def initialize(options = {})
      @opts = options[:opts]
      @db = options[:db]
    end

    def post_init
      @stash            = []   # 
      @data             = ""   # 
      @data_buf         = ""   # intermediary placeholder for buffering data as it's read for SETs
      @expected_length  = nil  # 
      @session_id       = @@next_session_id
      @@next_session_id = @@next_session_id + 1
      peer = Socket.unpack_sockaddr_in(get_peername)
      puts "--> (#{@session_id}) New client connection from #{peer[1]}:#{peer[0]}"
    end

    def receive_data(data)
      @data << data
      while line = @data.slice!(/.*?\r\n/m)
        response = process(line)
      end
      send_data response if response
    end

    def process(data)
      data = @data_buf + data if @data_buf.size > 0

      # our only non-normal state is consuming an object's data
      # when @expected_length is present
      if @expected_length && data.size == @expected_length
        response = set_data(data)
        @data_buf = ""
        return response
      elsif @expected_length
        @data_buf = data
        return
      end

      case data
        when SET_COMMAND
          set($1, $2, $3, $4.to_i)
        when GET_COMMAND
          get $1
        when DELETE_COMMAND
          delete $1
        when SHUTDOWN_COMMAND
          Server::shutdown
        when QUIT_COMMAND
          close_connection
          return nil
        when FLUSH_ALL_COMMAND
          flush_all
        else
          respond ERR_UNKNOWN_COMMAND
      end
    end

    #def receive_data(data)
    #  send_data(data)
    #  puts "<-- Data sent (#{@session_id})"
    #end

    def unbind
      puts "--X (#{@session_id}) Connection closed"
    end

    private

    def get(key)
      puts "--> (#{@session_id}) GET #{key}"
      data = @db[key]
      if !data.nil?
        puts "<-- (#{@session_id}) #{data.size}: " + data[0..30].gsub(/[\n\r]+/, ' ')
        respond GET_RESPONSE, key, 0, data.size, data
      else
        puts "<-- (#{@session_id}) " + NOT_FOUND_RESPONSE
        respond GET_RESPONSE_EMPTY
      end
    end

    def set(key, flags, expiry, len)
      @expected_length = len + 2  # to deal with the final \r\n
      @stash = [ key, flags, expiry ]
      nil
    end

    def set_data(data)
      key, flags, expiry = @stash
      value = data.slice(0...@expected_length-2)
      puts "--> (#{@session_id}) SET #{key} #{@expected_length-2}"
      @stash = []
      @expected_length = nil

      begin
        @db[key] = data
        puts "<-- (#{@session_id}) " + SET_RESPONSE_SUCCESS
        respond SET_RESPONSE_SUCCESS
      rescue
        puts "<-- (#{@session_id}) " + SET_RESPONSE_FAILURE
        respond SET_RESPONSE_FAILURE
      end
    end

    def delete(key)
      puts "--> (#{@session_id}) DELETE #{key}"
      if @db.has_key? key
        @db.delete key
        puts "<-- (#{@session_id}) " + DELETE_RESPONSE
        respond DELETE_RESPONSE
      else
        puts "<-- (#{@session_id}) " + NOT_FOUND_RESPONSE
        respond NOT_FOUND_RESPONSE
      end
    end

    def flush_all
      puts "--> (#{@session_id}) FLUSH_ALL"
      @db.clear
      puts "<-- (#{@session_id}) " + OK_RESPONSE
      respond OK_RESPONSE
    end

    def respond(str, *args)
      send_data sprintf(str, *args)
    end

  end # class ConnectionHandler

end # module MemcacheFS

# run the default server
server = MemcacheFS::Server.new
Signal.trap("INT") { puts "Terminating..."; server.stop }
server.start

