class Raft
  class Node
    include StateMachine
    ZERO = 0.to_bin.freeze

    state :follower, to: [:candidate] do
      @timer.delay = 150 + RandomBytes.uniform(150)
    end

    state :candidate, to: [:follower, :leader] do
      start_new_election
    end

    state :leader, to: [:follower] do
      @next_index = {}
      @match_index = {}
      next_index = 2
      if (last_entry = last_log_entry)
        next_index = last_entry[:index] + 1
      end
      @peers.each do |peer|
        @next_index[peer] = next_index
        @match_index[peer] = 0
      end
      send_heartbeat
      @timer.delay = 70
    end

    def initialize(options = {}, *args)
      @options = options
      @name = options.fetch(:name)
      @class = options.fetch(:class)
      @channel = "mruby-flotte-v1-#{@class}"
      @commit_index = 0
      @last_applied = 0
      @state = :follower
      @evasive = []
      @peers = {}
      @queued_requests = []
      @env = MDB::Env.new(maxdbs: 3)
      @env.open("#{@name}-#{@class}.lmdb", MDB::NOSUBDIR)
      @current_term_db = @env.database(MDB::INTEGERKEY | MDB::CREATE, "currentTerm")
      @voted_for_db = @env.database(MDB::INTEGERKEY | MDB::CREATE, "votedFor")
      @log_db = @env.database(MDB::INTEGERKEY | MDB::CREATE, "log")
      @instance = @class.new(*args)
      @reactor = CZMQ::Reactor.new
      @pipe = CZMQ::Zsock.new ZMQ::PAIR
      @reactor.poller(@pipe, &method(:pipe))
      @pipe.bind("inproc://#{object_id}-mruby-flotte-v1")
      @zyre = Zyre.new(@name)
      @reactor.poller(@zyre.socket, &method(:zyre))
      @zyre.start
    end

    def start
      @zyre.join(@channel)
      @timer ||= @reactor.timer(150 + RandomBytes.uniform(150), 0, &method(:timer))
      @reactor.run
    end

    private

    def append_entry(term, leader_id, prev_log_index, prev_log_term, entry, leader_commit)
      ct = current_term
      if term < ct
        return {rpc: :append_entry_reply, term: ct, success: false}
      elsif term > ct
        enter_new_term(term)
      end

      @timer.reset

      result = nil

      @log_db.cursor do |cursor|
        if (log_entry = cursor.set_key(prev_log_index.to_bin, nil, true))
          log_entry = MessagePack.unpack(log_entry.last)
          if log_entry[:term] != prev_log_term
            result = {rpc: :append_entry_reply, term: current_term, success: false}
            break
          end
          if log_entry[:term] == prev_log_term
            if log_entry[:index] == prev_log_index
              result = {rpc: :append_entry_reply, term: current_term, success: true}
            else
              result = {rpc: :append_entry_reply, term: current_term, success: false}
              break
            end
          end
        elsif cursor.last(nil, nil, true).nil?
          result = {rpc: :append_entry_reply, term: current_term, success: true}
        else
          result = {rpc: :append_entry_reply, term: current_term, success: false}
          break
        end

        if entry
          index = entry[:index].to_bin
          if (log_entry = cursor.set_key(index, nil, true))
            log_entry = MessagePack.unpack(log_entry.last)
            if log_entry[:term] != entry[:term]
              cursor.del
              while cursor.next(nil, nil, true)
                cursor.del
              end
            end
          end

          cursor.put(index, entry.to_msgpack, MDB::APPEND)

          if leader_commit > @commit_index
            @commit_index = [leader_commit, entry[:index]].min
          end
        end

        cursor.set_key(@last_applied.to_bin, nil, true)

        while @commit_index > @last_applied
          if (log_entry = cursor.next(nil, nil, true))
            log_entry = MessagePack.unpack(log_entry.last)
            @class.__send__(log_entry[:method], *log_entry[:args])
            @last_applied += 1
          else
            raise KeyError, "no such key: #{@last_applied}"
          end
        end
      end

      result
    end

    def append_entry_reply(term, follower_id, success)
      if term > ct
        enter_new_term(term)
      end
    end

    def request_vote(term, candidate_id, last_log_index, last_log_term)
      ct = current_term
      if term < ct
        return {rpc: :request_vote_reply, term: ct, vote_granted: false}
      elsif term > ct
        enter_new_term(term)
      end

      vf = voted_for
      if vf == nil || vf == candidate_id
        if (last_entry = last_log_entry)
          if last_log_index >= last_entry[:index]
            if last_log_term >= last_entry[:term]
              self.voted_for = candidate_id
              @timer.reset
              return {rpc: :request_vote_reply, term: current_term, vote_granted: true}
            end
          end
        else
          self.voted_for = candidate_id
          @timer.reset
          return {rpc: :request_vote_reply, term: current_term, vote_granted: true}
        end
      end

      {rpc: :request_vote_reply, term: current_term, vote_granted: false}
    end

    def request_vote_reply(term, candidate_id, vote_granted)
      if term > current_term
        enter_new_term(term)
      else
        if @state == :candidate && vote_granted
          if (@recieved_votes += 1) >= quorum
            transition :leader
          end
        end
      end
    end

    def start_new_election
      new_term = increment_current_term
      @voted_for_db[ZERO] = @name
      @recieved_votes = 1
      request_vote_rpc = {rpc: :request_vote, term: new_term}
      if (last_entry = last_log_entry)
        request_vote_rpc[:last_log_index] = last_entry[:index]
        request_vote_rpc[:last_log_term] = last_entry[:term]
      else
        request_vote_rpc[:last_log_index] = 0
        request_vote_rpc[:last_log_term] = 0
      end
      @zyre.shout(@channel, request_vote_rpc.to_msgpack)
      @timer.delay = 150 + RandomBytes.uniform(150)
    end

    def send_heartbeat
      append_entry_rpc = {rpc: :append_entry,
        term: current_term,
        entry: nil,
        leaderCommit: @commit_index}
      if (prev_entry = prev_log_entry)
        append_entry_rpc[:prev_log_index] = prev_entry[:index]
        append_entry_rpc[:prev_log_term] = prev_entry[:term]
      else
        append_entry_rpc[:prev_log_index] = 0
        append_entry_rpc[:prev_log_term] = 0
      end
      @zyre.shout(@channel, append_entry_rpc.to_msgpack)
    end

    def pipe(pipe_pi)
      msg = CZMQ::Zframe.recv(@pipe).to_str(true)
      payload = MessagePack.unpack(msg)
      case payload[:id]
      when ActorMessage::SEND_MESSAGE
        begin
          if (vf = voted_for)
            if vf == @name
            else
              @zyre.whisper(@peers[vf], msg)
            end
          end
          result = @instance.__send__(payload[:method], *payload[:args])
          @pipe.sendx({id: ActorMessage::SEND_OK, result: result}.to_msgpack)
        rescue => e
          @pipe.sendx({id: ActorMessage::ERROR, mrb_class: e.class, error: e.to_s}.to_msgpack)
        end
      when ActorMessage::ASYNC_SEND_MESSAGE
        begin
          @instance.__send__(payload[:method], *payload[:args])
        rescue => e
          CZMQ::Zsys.error(e.inspect)
        end
      end
    end

    def zyre(zyre_socket_pi)
      msg = @zyre.recv
      type = msg[0]
      uuid = msg[1]
      @evasive.delete(uuid)
      name = msg[2]
      case type
      when 'SHOUT'
        channel = msg[3]
        if (channel == @channel)
          request = MessagePack.unpack(msg[4])
          case request[:rpc]
          when :request_vote
            @zyre.whisper(uuid, request_vote(request[:term], name, request[:last_log_index], request[:last_log_term]).to_msgpack)
          when :append_entry
            @zyre.whisper(uuid, append_entry(request[:term], name, request[:prev_log_index], request[:prev_log_term], request[:entry], request[:leader_commit]).to_msgpack)
          end
        end
      when 'WHISPER'
        request = MessagePack.unpack(msg[3])
        case request[:rpc]
        when :request_vote_reply
          request_vote_reply(request[:term], name, request[:vote_granted])
        when :append_entry_reply
          append_entry_reply(request[:term], name, request[:success])
        when :call
          call(request[:method], *request[:args])
        end
      when 'JOIN'
        channel = msg[3]
        if channel == @channel
          @peers[name] = uuid
        end
      when 'EVASIVE'
        unless @evasive.include?(uuid)
          @evasive << uuid
        end
      when 'LEAVE'
        channel = msg[3]
        if channel == @channel
          @peers.delete(name)
        end
      when 'EXIT'
        @peers.delete(name)
      end
    end

    def timer(timer_id)
      case @state
      when :follower
        unless voted_for
          transition :candidate
        end
      when :candidate
        start_new_election
      when :leader
        send_heartbeat
      end
    end

    def quorum
      ((@peers.size - @evasive.size + 1) / 2).ceil
    end

    def current_term
      record = @current_term_db.first
      if record
        record.last.to_fix
      else
        0
      end
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

    def enter_new_term(new_term = nil)
      if new_term
        @current_term_db[ZERO] = new_term.to_bin
      else
        increment_current_term
      end
      @voted_for_db.del(ZERO)
      transition :follower
    end

    def voted_for
      @voted_for_db[ZERO]
    end

    def voted_for=(candidate_id)
      @voted_for_db[ZERO] = candidate_id
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
