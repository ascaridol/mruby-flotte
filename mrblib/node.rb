class Raft
  class Node
    ZERO = 0.to_bin

    def initialize(options = {})
      @env = MDB::Env.new(maxdbs: 3)
      @env.open(options.fetch(:logfile), MDB::NOSUBDIR)
      @current_term_db = @env.database(MDB::INTEGERKEY | MDB::CREATE, "currentTerm")
      @voted_for_db = @env.database(MDB::INTEGERKEY | MDB::CREATE, "votedFor")
      @log = @env.database(MDB::INTEGERKEY | MDB::CREATE, "log")
      @commit_index = 0
      @last_applied = 0
      @state = :follower
    end

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

    private

    def current_term
      record = @current_term_db.first(nil, nil, true)
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
      @voted_for_db.first(nil, nil, true)
    end

    def voted_for=(candidate_id)
      @voted_for_db[ZERO] = candidate_id
      candidate_id
    end

    def last_log_entry
      @log_db.last(nil, nil, true)
    end

    def increment_current_term
      record = @current_term_db.first(nil, nil, true)
      ct = 1
      if record
        ct = record.last.to_fix.succ
      end
      @current_term_db[ZERO] = ct.to_bin
      ct
    end
  end
end
