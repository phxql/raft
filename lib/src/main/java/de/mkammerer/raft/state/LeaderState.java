package de.mkammerer.raft.state;

import de.mkammerer.raft.NodeId;
import lombok.Value;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

@Value
public class LeaderState {
    /**
     * for each server, index of the next log entry to send to that server (initialized to leader last log index + 1).
     */
    Map<NodeId, Long> nextIndex;
    /**
     * for each server, index of highest log entry known to be replicated on server (initialized to 0, increases monotonically).
     */
    Map<NodeId, Long> matchIndex;

    public static LeaderState create(List<NodeId> nodes, long lastLogIndex) {
        Map<NodeId, Long> nextIndex = new HashMap<>();
        Map<NodeId, Long> matchIndex = new HashMap<>();

        for (NodeId node : nodes) {
            nextIndex.put(node, lastLogIndex + 1);
            matchIndex.put(node, 0L);
        }

        return new LeaderState(nextIndex, matchIndex);
    }
}
