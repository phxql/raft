package de.mkammerer.raft.state;

import lombok.AllArgsConstructor;
import lombok.Data;

import java.time.Duration;

@Data
@AllArgsConstructor
public class VolatileState {
    long commitIndex;
    long lastApplied;
    long lastHeartBeat;
    Role role;

    public static VolatileState create() {
        return new VolatileState(0, 0, 0, Role.FOLLOWER);
    }

    public void setLastHeartBeat(long now) {
        lastHeartBeat = now;
    }

    public boolean isHeartBeatExpired(Duration electionTimeout, long now) {
        Duration elapsed = Duration.ofNanos(now - lastHeartBeat);

        return elapsed.compareTo(electionTimeout) > 0;
    }

    public void incrementLastApplied() {
        lastApplied++;
    }

    public enum Role {
        FOLLOWER,
        CANDIDATE,
        LEADER
    }
}
