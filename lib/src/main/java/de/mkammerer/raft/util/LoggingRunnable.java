package de.mkammerer.raft.util;

import lombok.AllArgsConstructor;
import lombok.extern.slf4j.Slf4j;

@Slf4j
@AllArgsConstructor
public class LoggingRunnable implements Runnable {
    private final Runnable runnable;

    @Override
    public void run() {
        try {
            runnable.run();
        } catch (Throwable t) {
            log.error("", t);
            throw t;
        }
    }
}
