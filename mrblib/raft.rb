class Raft
  def initialize(options = {}, *args)
    @actor = Actor.new
    @node = @actor.init(Node, options, *args)
    @pipe = CZMQ::Zsock.new ZMQ::PAIR
    @pipe.connect("inproc://#{@node.object_id}-mruby-flotte-v1")
    @node.async_send(:start)
  end

  def send(method, *args)
    @pipe.sendx({rpc: :call, id: ActorMessage::SEND_MESSAGE, uuid: RandomBytes.buf(16), method: method, args: args}.to_msgpack)
    result = MessagePack.unpack(CZMQ::Zframe.recv(@pipe).to_str(true))
    case result[:id]
    when ActorMessage::SEND_OK
      result[:result]
    when ActorMessage::ERROR
      raise result[:mrb_class], result[:error]
    end
  end
end
