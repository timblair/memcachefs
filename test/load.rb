require 'rubygems'
require 'memcached'

cache = Memcached.new("localhost:33133")
cache.flush
STDOUT.sync = true

#
# create and store [lots of] key/value pairs
#   key   = 8 bytes
#   value = between 1 and 512 bytes
#

k = 12345678
100.times do |i|
  100.times do
    cache.set k.to_s, "x" * (rand(511) + 1)
    k += 1
  end
  print "#{i} "
end
