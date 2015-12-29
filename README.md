# mruby-flotte
raft consensus algorithm in mruby

WARNING
=======
this is almost completely untested, what is only working so far is log replication, log replay and state machine calls, but only under certain circumstances.

Prerequirements
===============
You need to have https://github.com/zeromq/zyre installed, installtion instructions are mostly the same as for https://github.com/zeromq/czmq#toc3-72.

Example
=======

```ruby
raft1 = Raft.new({name: '1', class: String}, 'hallo')
raft2 = Raft.new({name: '2', class: String}, 'hallo')
raft3 = Raft.new({name: '3', class: String}, 'hallo')

puts raft1.send(:to_s)
puts raft2.send(:to_s)
puts raft3.send(:to_s)
```
