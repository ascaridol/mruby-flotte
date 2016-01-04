class Raft
  class Storage
    ZERO = 0.to_bin.freeze

    def initialize(options = {})
      @db_name = "#{options.fetch(:name)}-#{options.fetch(:class)}.lmdb"
      @env = MDB::Env.new(maxdbs: 3)
      @env.open(@db_name, MDB::NOSUBDIR | MDB::NOTLS)
      @env.reader_check
      @current_term_db = @env.database(MDB::INTEGERKEY | MDB::CREATE, "currentTerm")
      @voted_for_db = @env.database(MDB::INTEGERKEY | MDB::CREATE, "votedFor")
      @log_db = @env.database(MDB::INTEGERKEY | MDB::CREATE, "log")
    end

    def current_term
      record = @current_term_db.first
      if record
        record.last.to_fix
      else
        0
      end
    end

    def current_term=(new_term)
      @current_term_db[ZERO] = new_term.to_bin
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

    def voted_for
      @voted_for_db[ZERO]
    end

    def voted_for=(candidate_id)
      @voted_for_db[ZERO] = candidate_id
    end

    def reset_voted_for
      @voted_for_db.del(ZERO)
    end

    def log_entry(index)
      if (log_entry = @log_db[index.to_bin])
        MessagePack.unpack(log_entry.last)
      end
    end

    def last_log_entry
      if (last_entry = @log_db.last)
        MessagePack.unpack(last_entry.last)
      end
    end

    def validate_request(request)
      return @log_db.empty? if request[:prev_log_index].nil? && request[:prev_log_term].nil?

      if (prev_log_entry = log_entry(request[:prev_log_index]))
        if prev_log_entry[:term] == request[:prev_log_term]
          true
        else
          false
        end
      else
        false
      end
    end

    def append_entry(entry)
      @log_db.cursor do |cursor|
        index = entry[:index].to_bin
        if (log_entry = cursor.set_key(index, nil, true))
          if MessagePack.unpack(log_entry.last)[:term] != entry[:term]
            cursor.del
            while cursor.next(nil, nil, true)
              cursor.del
            end
          end
        end

        cursor.put(index, entry.to_msgpack)
      end
    end

    def create_new_entry(payload)
      @log_db.cursor do |cursor|
        if (log_entry = cursor.last(nil, nil, true))
          payload[:index] = log_entry.first.to_fix.next
        else
          payload[:index] = 1
        end
        payload[:term] = current_term

        cursor.put(payload[:index].to_bin, payload.to_msgpack, MDB::APPEND)
      end
    end

    def build_log_replica(is_heartbeat, append_entry_rpc)
      @log_db.cursor(MDB::RDONLY) do |cursor|
        last_log_index = 0
        if (last_log_entry = cursor.last(nil, nil, true))
          last_log_index = last_log_entry.first.to_fix
          if is_heartbeat
            last_log_entry = MessagePack.unpack(last_log_entry.last)
            append_entry_rpc[:prev_log_index] = last_log_entry[:index]
            append_entry_rpc[:prev_log_term] = last_log_entry[:term]
          else
            if (prev_log_entry = cursor.prev(nil, nil, true))
              prev_log_entry = MessagePack.unpack(prev_log_entry.last)
              append_entry_rpc[:prev_log_index] = prev_log_entry[:index]
              append_entry_rpc[:prev_log_term] = prev_log_entry[:term]
            end
            append_entry_rpc[:entry] = MessagePack.unpack(last_log_entry.last)
          end
        elsif !is_heartbeat
          raise LogError, "cannot replicate empty log"
        end

        last_log_index
      end
    end
  end
end
