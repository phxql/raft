package de.mkammerer.raft;

import java.time.Duration;
import java.util.concurrent.ThreadLocalRandom;

public class Random {
    private static final Duration MIN = Duration.ofMillis(150);
    private static final Duration MAX = Duration.ofMillis(300);

    public Duration getElectionTimeout() {
        return Duration.ofMillis(ThreadLocalRandom.current().nextLong(MIN.toMillis(), MAX.toMillis()));
    }

    public Duration getHeartBeat() {
        return MIN.dividedBy(2);
    }
}
