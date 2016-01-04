class Raft
  def initialize(options = {}, *args)
    @actor = Actor.new
    @node = @actor.init(Node, options, *args)
    @pipe = CZMQ::Zsock.new ZMQ::PAIR
    @pipe.connect("inproc://#{@node.object_id}-mruby-flotte-v1")
    @node.async_send(:start)
  end

  def send(method, *args)
    @pipe.sendx({rpc: :call, id: ActorMessage::SEND_MESSAGE, uuid: RandomBytes.buf(16), command: {method: method, args: args}}.to_msgpack)
    response = MessagePack.unpack(CZMQ::Zframe.recv(@pipe).to_str(true))
    case response[:id]
    when ActorMessage::SEND_OK
      response[:result]
    when ActorMessage::ERROR
      raise response[:mrb_class], response[:error]
    end
  end
end
