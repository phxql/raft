package de.mkammerer.raft;

import java.time.Duration;
import java.util.concurrent.ThreadLocalRandom;

public class Random {

    private static final Duration MIN = Duration.ofMillis(1500);
    private static final Duration MAX = Duration.ofMillis(3000);

    public Duration getElectionTimeout() {
        return Duration.ofMillis(ThreadLocalRandom.current().nextLong(MIN.toMillis(), MAX.toMillis()));
    }
}
