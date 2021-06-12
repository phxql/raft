package de.mkammerer.raft.rpc;

import de.mkammerer.raft.NodeId;
import lombok.Value;

@Value
public class RequestVoteRpc {
    /**
     * candidate’s term.
     */
    long term;
    /**
     * candidate requesting vote.
     */
    NodeId candidateId;
    /**
     * index of candidate’s last log entry.
     */
    long lastLogIndex;
    /**
     * term of candidate’s last log entry.
     */
    long lastLogTerm;

    @Value
    public static class Result {
        /**
         * currentTerm, for candidate to update itself.
         */
        long term;
        /**
         * true means candidate received vote.
         */
        boolean voteGranted;

        public static Result no(long term) {
            return new Result(term, false);
        }

        public static Result yes(long term) {
            return new Result(term, true);
        }
    }
}
