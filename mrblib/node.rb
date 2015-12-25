class Raft
  class Node
    include StateMachine
    ZERO = 0.to_bin

    state :follower, to: [:candidate]

    state :candidate, to: [:follower, :leader] do
      start_new_election
    end

    state :leader, to: [:follower] do
      @nextIndex = {}
      @matchIndex = {}
      if (last_entry = last_log_entry)
        next_index = last_entry[:index] + 1
        @zyre.peers.each do |peer|
          @nextIndex[peer] = next_index
        end
      end
      @timer.delay = 75
    end

    def initialize(options = {}, *args)
      @options = options
      @name = options.fetch(:name)
      @class = options.fetch(:class)
      @channel = "mruby-flotte-v1-#{@class}"
      @commitIndex = 0
      @lastApplied = 0
      @state = :follower
      @env = MDB::Env.new(maxdbs: 3)
      @env.open("#{@name}-#{@class}.lmdb", MDB::NOSUBDIR)
      @current_term_db = @env.database(MDB::INTEGERKEY | MDB::CREATE, "currentTerm")
      @voted_for_db = @env.database(MDB::INTEGERKEY | MDB::CREATE, "votedFor")
      @log_db = @env.database(MDB::INTEGERKEY | MDB::CREATE, "log")
      @instance = @class.new(*args)
      @reactor = CZMQ::Reactor.new
      @pipe = CZMQ::Zsock.new ZMQ::PAIR
      @pipe.bind("inproc://#{object_id}-mruby-flotte-v1")
      @reactor.poller(@pipe, &method(:pipe))
      @zyre = Zyre.new(@name)
      @uuid = @zyre.uuid
      @evasive = []
      @reactor.poller(@zyre.socket, &method(:zyre))
    end

    def start
      @zyre.start
      @zyre.join(@channel)
      @timer ||= @reactor.timer(150 + RandomBytes.uniform(150), 0, &method(:timer))
      @reactor.run
    end

    private

    def append_entry(term, leader_id, prev_log_index, prev_log_term, entry, leader_commit)
      @timer.reset
      ct = current_term
      if term < ct
        return {rpc: :append_entry_reply, term: ct, success: false}
      elsif term > ct
        self.current_term = term
        transition :follower
      end
    end

    def request_vote(term, candidate_id, last_log_index, last_log_term)
      @timer.reset
      ct = current_term
      if term < ct
        return {rpc: :request_vote_reply, term: ct, vote_granted: false}
      elsif term > ct
        self.current_term = term
        transition :follower
      end

      vf = voted_for(term)
      if vf == nil || vf == candidate_id
        if (last_entry = last_log_entry)
          if last_log_index >= last_entry[:index]
            if last_log_term >= last_entry[:term]
              self.voted_for = candidate_id
              return {rpc: :request_vote_reply, term: current_term, vote_granted: true}
            end
          end
        else
          self.voted_for = candidate_id
          return {rpc: :request_vote_reply, term: current_term, vote_granted: true}
        end
      end

      {rpc: :request_vote_reply, term: current_term, vote_granted: false}
    end

    def start_new_election
      new_term = increment_current_term
      @voted_for_db[new_term.to_bin] = @uuid
      @recieved_votes = 1
      request_vote_rpc = {rpc: :request_vote, term: new_term}
      if (last_entry = last_log_entry)
        request_vote_rpc[:lastLogIndex] = last_entry[:index]
        request_vote_rpc[:lastLogTerm] = last_entry[:term]
      else
        request_vote_rpc[:lastLogIndex] = 0
        request_vote_rpc[:lastLogTerm] = 0
      end
      @zyre.shout(@channel, request_vote_rpc.to_msgpack)
      @timer.delay = 150 + RandomBytes.uniform(150)
    end

    def pipe(pipe_pi)
      msg = MessagePack.unpack(CZMQ::Zframe.recv(@pipe).to_str(true))
      case msg[:id]
      when ActorMessage::SEND_MESSAGE
        begin
          result = @instance.__send__(msg[:method], *msg[:args])
          @pipe.sendx({id: ActorMessage::SEND_OK, result: result}.to_msgpack)
        rescue => e
          @pipe.sendx({id: ActorMessage::ERROR, mrb_class: e.class, error: e.to_s}.to_msgpack)
        end
      when ActorMessage::ASYNC_SEND_MESSAGE
        begin
          @instance.__send__(msg[:method], *msg[:args])
        rescue => e
          CZMQ::Zsys.error(e.inspect)
        end
      end
    end

    def zyre(zyre_socket_pi)
      msg = @zyre.recv
      type = msg[0]
      case type
      when 'SHOUT'
        uuid = msg[1]
        channel = msg[3]
        if (channel == @channel)
          request = MessagePack.unpack(msg[4])
          case request[:rpc]
          when :request_vote
            @zyre.whisper(uuid, request_vote(request[:term], uuid, request[:last_log_index], request[:last_log_term]).to_msgpack)
          when :append_entry
            @zyre.whisper(uuid, append_entry(request[:term], uuid, request[:prev_log_index], request[:prev_log_term], request[:entry], request[:leader_commit]).to_msgpack)
          end
        end
      end
    end

    def timer(timer_id)
      case @state
      when :follower
        unless voted_for(current_term)
          transition :candidate
        end
      when :candidate
        start_new_election
      when :leader
        append_entry_rpc = {rpc: :append_entry,
          term: current_term,
          entry: nil,
          leaderCommit: @commitIndex}
        if (prev_entry = prev_log_entry)
          append_entry_rpc[:prevLogIndex] = prev_entry[:index]
          append_entry_rpc[:prevLogTerm] = prev_entry[:term]
        else
          append_entry_rpc[:prevLogIndex] = 0
          append_entry_rpc[:prevLogTerm] = 0
        end
        @zyre.shout(@channel, append_entry_rpc.to_msgpack)
      end
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

    def increment_current_term
      record = @current_term_db.first
      ct = 1
      if record
        ct = record.last.to_fix + 1
      end
      @current_term_db[ZERO] = ct.to_bin
      ct
    end

    def voted_for(term)
      if (vf = @voted_for_db[term.to_bin])
        vf.last
      else
        nil
      end
    end

    def voted_for=(candidate_id)
      @voted_for_db[@current_term_db.first.first] = candidate_id
      candidate_id
    end

    def last_log_entry
      if (last_entry = @log_db.last)
        MessagePack.unpack(last_entry.last)
      else
        nil
      end
    end

    def prev_log_entry
      entry = nil
      @log_db.cursor(MDB::RDONLY) do |cursor|
        if cursor.last(nil, nil, true)
          if (prev_entry = cursor.prev(nil, nil, true))
            entry = MessagePack.unpack(prev_entry.last)
          end
        end
      end
      entry
    end
  end
end
