class Raft
  class Node
    include StateMachine

    state :follower, to: [:candidate] do
      @timer.delay = 150 + RandomBytes.uniform(150)
    end

    state :candidate, to: [:follower, :leader] do
      start_new_election
    end

    state :leader, to: [:follower] do
      puts "#{@name} is the leader"
      next_index = 1
      if (last_entry = @storage.last_log_entry)
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
      @storage = Storage.new(options)
      @instance = @class.new(*args)
      @reactor = CZMQ::Reactor.new
      @pipe = CZMQ::Zsock.new ZMQ::PAIR
      @reactor.poller(@pipe, &method(:pipe))
      @pipe.bind("inproc://#{object_id}-mruby-flotte-v1")
      @zyre = Zyre.new(@name)
      @reactor.poller(@zyre.socket, &method(:zyre))
      @zyre.start
      @zyre.join(@channel)
      @timer = @reactor.timer(150 + RandomBytes.uniform(150), 0, &method(:timer))
    end

    def start
      @reactor.run
    end

    private

    def append_entry_rpc(leader_id, request)
      ct = @storage.current_term
      term = request[:term]
      if term < ct
        return {rpc: :append_entry_reply, term: ct, success: false}
      elsif term > ct
        enter_new_term(term)
      end

      @leader_id = leader_id
      @timer.reset

      success = @storage.validate_request(request)

      if success
        entry = request[:entry]
        last_index = nil
        if entry
          @storage.append_entry(entry)
          last_index = entry[:index]
        end

        leader_commit = request[:leader_commit]
        if leader_commit > @commit_index
          if last_index
            @commit_index = [leader_commit, last_index].min
          else
            @commit_index = [leader_commit, @storage.last_log_entry[:index]].min
          end
        end

        while @commit_index > @last_applied
          log_entry = @storage.log_entry(@last_applied += 1)
          response = @instance.__send__(log_entry[:command][:method], *log_entry[:command][:args])
          if @pipe_awaits == log_entry[:uuid]
            @pipe.sendx({id: ActorMessage::SEND_OK, result: response}.to_msgpack)
            @pipe_awaits = false
          end
        end
      end

      {rpc: :append_entry_reply, term: @storage.current_term, success: success}
    end

    def append_entry_reply(follower_id, response)
      ct = @storage.current_term
      term = response[:term]
      if term > ct
        enter_new_term(term)
      end

      if leader?
        peer = @peers[follower_id]
        if response[:success]
          index = peer[:next_index]
          if index > @commit_index && (log_entry = @storage.log_entry(index))
            peer[:next_index] += 1
            peer[:match_index] = index
            if log_entry[:term] == ct
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
        else
          peer[:next_index] -= 1
          if (log_entry = @storage.log_entry(peer[:next_index]))
            append_entry_rpc = {rpc: :append_entry, term: ct, entry: log_entry, leader_commit: @commit_index}
            if (prev_log_entry = @storage.log_entry(log_entry[:index] - 1))
              append_entry_rpc[:prev_log_index] = prev_log_entry[:index]
              append_entry_rpc[:prev_log_term] = prev_log_entry[:term]
            end
            @zyre.whisper(peer[:uuid], append_entry_rpc.to_msgpack)
          end
        end
      end

      while @commit_index > @last_applied
        log_entry = @storage.log_entry(@last_applied += 1)
        response = @instance.__send__(log_entry[:command][:method], *log_entry[:command][:args])
        if @pipe_awaits == log_entry[:uuid]
          @pipe.sendx({id: ActorMessage::SEND_OK, result: response}.to_msgpack)
          @pipe_awaits = false
        end
      end
    end

    def request_vote_rpc(candidate_id, request)
      ct = @storage.current_term
      term = request[:term]
      if term < ct
        return {rpc: :request_vote_reply, term: ct, vote_granted: false}
      elsif term > ct
        enter_new_term(term)
      end

      vote_granted = false

      vf = @storage.voted_for
      if vf == nil || vf == candidate_id
        if (last_log_entry = @storage.last_log_entry)
          if request[:last_log_index] >= last_log_entry[:index] && request[:last_log_term] >= last_log_entry[:term]
            @storage.voted_for = candidate_id
            @timer.reset
            vote_granted = true
          end
        else
          @storage.voted_for = candidate_id
          @timer.reset
          vote_granted = true
        end
      end

      {rpc: :request_vote_reply, term: @storage.current_term, vote_granted: vote_granted}
    end

    def request_vote_reply(candidate_id, request)
      term = request[:term]
      if term > @storage.current_term
        enter_new_term(term)
      elsif candidate? && request[:vote_granted] && (@recieved_votes += 1) >= quorum
        transition :leader
      end
    end

    def pipe(pipe_pi)
      msg = CZMQ::Zframe.recv(@pipe).to_str
      payload = MessagePack.unpack(msg)
      append_entry(msg, payload, true)
    end

    SHOUT = 'SHOUT'
    WHISPER = 'WHISPER'
    JOIN = 'JOIN'
    EVASIVE = 'EVASIVE'
    LEAVE = 'LEAVE'
    EXIT = 'EXIT'

    def zyre(zyre_socket_pi)
      msg = @zyre.recv
      type, uuid, name = msg[0], msg[1], msg[2]
      @evasive.delete(uuid)
      case type
      when SHOUT
        channel = msg[3]
        if (channel == @channel)
          request = MessagePack.unpack(msg[4])
          case request[:rpc]
          when :request_vote
            @zyre.whisper(uuid, request_vote_rpc(name, request).to_msgpack)
          when :append_entry
            @zyre.whisper(uuid, append_entry_rpc(name, request).to_msgpack)
          end
        end
      when WHISPER
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
      when JOIN
        channel = msg[3]
        if channel == @channel
          if leader?
            next_index = 1
            if (last_entry = @storage.last_log_entry)
              next_index = last_entry[:index] + 1
            end
            @peers[name] = {uuid: uuid, next_index: next_index, match_index: 0}
          else
            @peers[name] = {uuid: uuid}
          end
        end
      when EVASIVE
        unless @evasive.include?(uuid)
          @evasive << uuid
        end
      when LEAVE
        channel = msg[3]
        if channel == @channel
          @peers.delete(name)
          transition :candidate if @leader_id == name
        end
      when EXIT
        @peers.delete(name)
        transition :candidate if @leader_id == name
      end
    end

    def timer(timer_id)
      case @state
      when :follower
        transition :candidate
      when :candidate
        start_new_election
      when :leader
        replicate_log(true)
      end
    end

    def enter_new_term(new_term = nil)
      if new_term
        @storage.current_term = new_term
      else
        @storage.increment_current_term
      end
      @storage.reset_voted_for
      @leader_id = nil
      transition :follower
    end

    def append_entry(msg, payload, from_pipe = false)
      if from_pipe
        @pipe_awaits = payload[:uuid]
      end
      if leader?
        @storage.create_new_entry(payload)
        replicate_log
      elsif @leader_id
        @zyre.whisper(@peers[@leader_id][:uuid], msg)
      else
        raise Error, "no leader elected"
      end
    end

    def start_new_election
      new_term = @storage.increment_current_term
      @storage.voted_for = @name
      @recieved_votes = 1
      request_vote_rpc = {rpc: :request_vote, term: new_term}
      if (last_entry = @storage.last_log_entry)
        request_vote_rpc[:last_log_index] = last_entry[:index]
        request_vote_rpc[:last_log_term] = last_entry[:term]
      else
        request_vote_rpc[:last_log_index] = 0
        request_vote_rpc[:last_log_term] = 0
      end
      @zyre.shout(@channel, request_vote_rpc.to_msgpack)
      @timer.delay = 150 + RandomBytes.uniform(150)
    end

    def replicate_log(is_heartbeat = false)
      append_entry_rpc = {rpc: :append_entry, term: @storage.current_term, leader_commit: @commit_index}
      last_log_index = @storage.build_log_replica(is_heartbeat, append_entry_rpc)
      append_entry_rpc = append_entry_rpc.to_msgpack

      if is_heartbeat
        @zyre.shout(@channel, append_entry_rpc)
      else
        @peers.each_value do |peer|
          if (last_log_index >= peer[:next_index])
            @zyre.whisper(peer[:uuid], append_entry_rpc)
          end
        end
      end
    end

    def quorum
      ((@peers.size - @evasive.size + 1) / 2).ceil
    end

  end
end
