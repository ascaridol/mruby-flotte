# mruby-flotte
raft consensus algorithm in mruby

WARNING
=======
this is almost completely untested, what is working so far is leader election, log replication, log replay and state machine calls, but only tested under certain circumstances.

Prerequirements
===============
You need to have https://github.com/zeromq/zyre installed, installtion instructions are mostly the same as for https://github.com/zeromq/czmq#toc3-72.

Example
=======

```ruby
raft1 = Raft.new({name: '1', class: Raft::Counter})
raft2 = Raft.new({name: '2', class: Raft::Counter})
raft3 = Raft.new({name: '3', class: Raft::Counter})
raft4 = Raft.new({name: '4', class: Raft::Counter})
raft5 = Raft.new({name: '5', class: Raft::Counter})

CZMQ::Zclock.sleep(3000)

puts raft1.send(:incr)
puts raft2.send(:incr)
puts raft3.send(:incr)
puts raft4.send(:incr)
puts raft5.send(:incr)

puts raft1.send(:counter)
puts raft2.send(:counter)
puts raft3.send(:counter)
puts raft4.send(:counter)
puts raft5.send(:counter)

```
