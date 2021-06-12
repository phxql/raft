package de.mkammerer.raft.rpc;

import de.mkammerer.raft.NodeId;
import de.mkammerer.raft.state.Log;
import lombok.Value;

import java.util.List;

@Value
public class AppendEntriesRpc {
    /**
     * leader’s term.
     */
    long term;
    /**
     * so follower can redirect clients.
     */
    NodeId leaderId;
    /**
     * index of log entry immediately preceding new ones.
     */
    long prevLogIndex;
    /**
     * term of prevLogIndex entry.
     */
    long prevLogTerm;
    /**
     * log entries to store (empty for heartbeat; may send more than one for efficiency)
     */
    List<Log.Entry> entries;
    /**
     * leader’s commitIndex.
     */
    long leaderCommit;

    @Value
    public static class Result {
        /**
         * currentTerm, for leader to update itself.
         */
        long term;
        /**
         * true if follower contained entry matching prevLogIndex and prevLogTerm.
         */
        boolean success;

        public static Result fail(long term) {
            return new Result(term, false);
        }

        public static Result ok(long term) {
            return new Result(term, true);
        }
    }
}
