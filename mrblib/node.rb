class Raft
  class Error < StandardError; end
  class LogError < Error; end

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
      puts "#{@name} is the leader"
      next_index = 1
      if (last_entry = last_log_entry)
        next_index = last_entry[:index] + 1
      end
      @peers.each_value do |peer|
        peer[:next_index] = next_index
        peer[:match_index] = 0
      end
      @leader_id = @name
      replicate_log(true)
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
      @zyre.join(@channel)
    end

    def start
      @timer = @reactor.timer(150 + RandomBytes.uniform(150), 0, &method(:timer))
      @reactor.run
    end

    private

    def append_entry_rpc(leader_id, request)
      ct = current_term
      term = request[:term]
      if term < ct
        return {rpc: :append_entry_reply, term: ct, success: false}
      elsif term > ct
        enter_new_term(term)
      end

      @leader_id = leader_id
      @timer.reset

      result = {rpc: :append_entry_reply, term: current_term}

      @log_db.cursor do |cursor|
        prev_log_index = request[:prev_log_index]
        prev_log_term = request[:prev_log_term]
        leader_commit = request[:leader_commit]
        entry = request[:entry]
        if (prev_log_entry = cursor.set_key(prev_log_index.to_bin, nil, true))
          prev_log_entry = MessagePack.unpack(prev_log_entry.last)
          if prev_log_entry[:term] != prev_log_term
            result[:success] = false
            break
          end
          if prev_log_entry[:term] == prev_log_term
            if prev_log_entry[:index] == prev_log_index
              result[:success] = true
            else
              result[:success] = false
              break
            end
          end
        elsif cursor.last(nil, nil, true).nil? || (prev_log_term == 0 && prev_log_index == 0)
          result[:success] = true
        else
          result[:success] = false
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
        end
        if leader_commit > @commit_index
          if entry
            @commit_index = [leader_commit, entry[:index]].min
          else
            @commit_index = leader_commit
          end
        end

        while @commit_index > @last_applied
          @last_applied += 1
          if (log_entry = cursor.set_key(@last_applied.to_bin, nil, true))
            log_entry = MessagePack.unpack(log_entry.last)
            response = @instance.__send__(log_entry[:command][:method], *log_entry[:command][:args])
            if @pipe_awaits == log_entry[:uuid]
              @pipe.sendx({id: ActorMessage::SEND_OK, result: response}.to_msgpack)
              @pipe_awaits = false
            end
          else
            @last_applied -= 1
            break
          end
        end
      end

      result
    end

    def append_entry_reply(follower_id, request)
      ct = current_term
      term = request[:term]
      if term > ct
        enter_new_term(term)
      elsif request[:success]
        index = @peers[follower_id][:next_index]
        if index > @commit_index && (log_entry = @log_db[index.to_bin])
          entry = MessagePack.unpack(log_entry.last)
          @peers[follower_id][:next_index] += 1
          @peers[follower_id][:match_index] = index
          if entry[:term] == ct
            matched_indexes = 1
            @peers.each_value do |peer|
              if peer[:match_index] >= index && (matched_indexes += 1) >= quorum
                @commit_index = index
                replicate_log(true)
                @timer.reset
                break
              end
            end
          end
        end
      elsif @peers[follower_id][:next_index] > 1 && (log_entry = @log_db[(@peers[follower_id][:next_index] -= 1).to_bin])
        log_entry = MessagePack.unpack(log_entry.last)
        append_entry_rpc = {rpc: :append_entry, term: current_term, prev_log_index: log_entry[:index] - 1, entry: log_entry, leader_commit: @commit_index}
        if (prev_log_entry = @log_db[append_entry_rpc[:prev_log_index].to_bin])
          prev_log_entry = MessagePack.unpack(prev_log_entry.last)
          append_entry_rpc[:prev_log_term] = prev_log_entry[:term]
        else
          append_entry_rpc[:prev_log_term] = 0
        end
        @zyre.whisper(@peers[follower_id][:uuid], append_entry_rpc.to_msgpack)
      end

      @log_db.cursor(MDB::RDONLY) do |cursor|
        while @commit_index > @last_applied
          @last_applied += 1

          if (log_entry = cursor.set_key(@last_applied.to_bin, nil, true))
            log_entry = MessagePack.unpack(log_entry.last)
            response = @instance.__send__(log_entry[:command][:method], *log_entry[:command][:args])
            if @pipe_awaits == log_entry[:uuid]
              @pipe.sendx({id: ActorMessage::SEND_OK, result: response}.to_msgpack)
              @pipe_awaits = false
            end
          else
            @last_applied -= 1
            break
          end
        end
      end
    end

    def request_vote_rpc(candidate_id, request)
      ct = current_term
      term = request[:term]
      if term < ct
        return {rpc: :request_vote_reply, term: ct, vote_granted: false}
      elsif term > ct
        enter_new_term(term)
      end

      vf = voted_for
      if vf == nil || vf == candidate_id
        if (last_entry = last_log_entry)
          if request[:last_log_index] >= last_entry[:index] && request[:last_log_term] >= last_entry[:term]
            self.voted_for = candidate_id
            @timer.reset
            return {rpc: :request_vote_reply, term: current_term, vote_granted: true}
          end
        else
          self.voted_for = candidate_id
          @timer.reset
          return {rpc: :request_vote_reply, term: current_term, vote_granted: true}
        end
      end

      {rpc: :request_vote_reply, term: current_term, vote_granted: false}
    end

    def request_vote_reply(candidate_id, request)
      term = request[:term]
      if term > current_term
        enter_new_term(term)
      elsif candidate? && request[:vote_granted] && (@recieved_votes += 1) >= quorum
        transition :leader
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

    def pipe(pipe_pi)
      msg = CZMQ::Zframe.recv(@pipe).to_str
      payload = MessagePack.unpack(msg)
      append_entry(msg, payload, true)
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
            @zyre.whisper(uuid, request_vote_rpc(name, request).to_msgpack)
          end
        end
      when 'WHISPER'
        request = MessagePack.unpack(msg[3])
        case request[:rpc]
        when :request_vote_reply
          request_vote_reply(name, request)
        when :append_entry_reply
          append_entry_reply(name, request)
        when :append_entry
          @zyre.whisper(uuid, append_entry_rpc(name, request).to_msgpack)
        when :call
          append_entry(msg, request)
        end
      when 'JOIN'
        channel = msg[3]
        if channel == @channel
          if leader?
            next_index = 1
            if (last_entry = last_log_entry)
              next_index = last_entry[:index] + 1
            end
            @peers[name] = {uuid: uuid, next_index: next_index, match_index: 0}
          else
            @peers[name] = {uuid: uuid}
          end
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
        unless @leader_id
          transition :candidate
        end
      when :candidate
        start_new_election
      when :leader
        replicate_log(true)
      end
    end

    def append_entry(msg, payload, from_pipe = false)
      if from_pipe
        @pipe_awaits = payload[:uuid]
      end
      if leader?
        @log_db.cursor do |cursor|
          if (log_entry = cursor.last(nil, nil, true))
            payload[:index] = log_entry.first.to_fix.next
          else
            payload[:index] = 1
          end
          payload[:term] = current_term
          cursor.put(payload[:index].to_bin, payload.to_msgpack, MDB::APPEND)
        end
        replicate_log
      elsif @leader_id
        @zyre.whisper(@peers[@leader_id][:uuid], msg)
      else
        raise Error, "no leader elected"
      end
    end

    def replicate_log(is_heartbeat = false)
      append_entry_rpc = {rpc: :append_entry, term: current_term, leader_commit: @commit_index}
      last_log_index = 0

      @log_db.cursor(MDB::RDONLY) do |cursor|
        if (last_entry = cursor.last(nil, nil, true))
          if is_heartbeat
            last_log_index = last_entry.first.to_fix
          else
            append_entry_rpc[:entry] = MessagePack.unpack(last_entry.last)
            last_log_index = append_entry_rpc[:entry][:index]
          end
          if (prev_log_entry = cursor.prev(nil, nil, true))
            prev_log_entry = MessagePack.unpack(prev_log_entry.last)
            append_entry_rpc[:prev_log_index] = prev_log_entry[:index]
            append_entry_rpc[:prev_log_term] = prev_log_entry[:term]
          else
            append_entry_rpc[:prev_log_index] = 0
            append_entry_rpc[:prev_log_term] = 0
          end
        elsif is_heartbeat
          append_entry_rpc[:prev_log_index] = 0
          append_entry_rpc[:prev_log_term] = 0
        else
          raise LogError, "cannot replicate empty log"
        end
      end

      append_entry_rpc = append_entry_rpc.to_msgpack

      @peers.each_value do |peer|
        if last_log_index >= peer[:next_index]
          @zyre.whisper(peer[:uuid], append_entry_rpc)
        elsif is_heartbeat
          @zyre.whisper(peer[:uuid], append_entry_rpc)
        end
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
      @leader_id = nil
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
  end
end
