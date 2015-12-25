class Raft
  class Node
    include StateMachine
    ZERO = 0.to_bin

    state :follower, to: [:candidate]
    state :candidate, to: [:follower, :leader] do
      @voted_for_db[ZERO] = @name
      @recieved_votes = 1
    end
    state :leader, to: [:follower]

    def initialize(options = {}, *args)
      @options = options.dup
      @name = @options.fetch(:name)
      @class = @options.fetch(:class)
      @env = MDB::Env.new(maxdbs: 3)
      @env.open("#{@class}.lmdb", MDB::NOSUBDIR)
      @current_term_db = @env.database(MDB::INTEGERKEY | MDB::CREATE, "currentTerm")
      @voted_for_db = @env.database(MDB::INTEGERKEY | MDB::CREATE, "votedFor")
      @log_db = @env.database(MDB::INTEGERKEY | MDB::CREATE, "log")
      @actor_message = ActorMessage.new
      @channel = "mruby-flotte-v1-#{@class}"
      @instance = @class.new(*args)
      @state = :follower
      @reactor = CZMQ::Reactor.new
      @pipe = CZMQ::Zsock.new ZMQ::PAIR
      @pipe.bind("inproc://#{object_id}")
      @reactor.poller(@pipe, &method(:pipe))
      @zyre = Zyre.new(@name)
      @reactor.poller(@zyre.socket, &method(:zyre))
    end

    def start
      @zyre.start
      @zyre.join(@channel)
      @timer ||= @reactor.timer(150 + RandomBytes.uniform(150), 0, &method(:timer))
      @reactor.run
    end

    private

    def append_entry(term, leader_id, prev_log_index, prev_log_term, entries, leader_commit)
    end

    def request_vote(term, candidate_id, last_log_index, last_log_term)
      ct = current_term
      if term < ct
        return {term: ct, vote_granted: false}
      end

      vf = voted_for
      if vf == nil || vf.last == candidate_id
        last_entry = last_log_entry
        if last_entry
          if last_log_index >= last_entry.first.to_fix
            entry = MessagePack.unpack(last_entry.last)
            if last_log_term >= entry[:term]
              voted_for = candidate_id
              return {term: ct, vote_granted: true}
            end
          end
        end
      end

      {term: ct, vote_granted: false}
    end

    def pipe(pipe_pi)
      @actor_message.recv(@pipe)
      case @actor_message.id
      when ActorMessage::SEND_MESSAGE
        begin
          result = @instance.__send__(@actor_message.method, *MessagePack.unpack(@actor_message.args))
          @actor_message.result = result.to_msgpack
          @actor_message.id = ActorMessage::SEND_OK
          @actor_message.send(@pipe)
        rescue => e
          @actor_message.mrb_class = String(e.class)
          @actor_message.error = String(e)
          @actor_message.id = ActorMessage::ERROR
          @actor_message.send(@pipe)
        end
      when ActorMessage::ASYNC_SEND_MESSAGE
        begin
          @instance.__send__(@actor_message.method, *MessagePack.unpack(@actor_message.args))
        rescue => e
          CZMQ::Zsys.error(e.inspect)
        end
      end
    end

    def zyre(zyre_socket_pi)
      msg = @zyre.recv
    end

    def timer(timer_id)
    end

    def current_term
      record = @current_term_db.first
      if record
        record.last.to_fix
      else
        0
      end
    end

    def current_term=(term)
      @current_term_db[ZERO] = term.to_bin
      term
    end

    def voted_for
      @voted_for_db.first
    end

    def voted_for=(candidate_id)
      @voted_for_db[ZERO] = candidate_id
      candidate_id
    end

    def last_log_entry
      @log_db.last
    end

    def increment_current_term
      record = @current_term_db.first
      ct = 1
      if record
        ct = record.last.to_fix.succ
      end
      @current_term_db[ZERO] = ct.to_bin
      ct
    end
  end
end
