package de.mkammerer.raft.util;

public final class Assert {
    private Assert() {
    }

    public static void that(boolean condition, String message) {
        if (!condition) {
            throw new AssertionError(message);
        }
    }
}
