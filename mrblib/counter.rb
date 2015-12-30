class Raft
  class Counter
    attr_reader :counter

    def initialize
      @counter = 0
    end

    def incr(by = 1)
      @counter += by
    end

    def decr(by = 1)
      @counter -= by
    end
  end
end
