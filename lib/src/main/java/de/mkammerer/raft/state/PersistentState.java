package de.mkammerer.raft.state;

import de.mkammerer.raft.NodeId;
import de.mkammerer.raft.util.Assert;
import lombok.AllArgsConstructor;
import lombok.Data;
import org.checkerframework.checker.nullness.qual.Nullable;

@Data
@AllArgsConstructor
public class PersistentState {
    /**
     * latest term server has seen (initialized to 0 on first boot, increases monotonically).
     */
    long currentTerm;
    /**
     * candidateId that received vote in current term (or null if none).
     */
    @Nullable
    NodeId votedFor;
    /**
     * log entries; each entry contains command for state machine, and term when entry was received by leader (first index is 1).
     */
    Log log;

    public static PersistentState create() {
        return new PersistentState(0L, null, Log.empty());
    }

    public void setCurrentTerm(long newValue) {
        Assert.that(newValue > this.currentTerm, "newValue > this.currentTerm");
        votedFor = null;
        this.currentTerm = newValue;
    }

    public void incrementCurrentTerm() {
        currentTerm++;
        votedFor = null;
    }
}
