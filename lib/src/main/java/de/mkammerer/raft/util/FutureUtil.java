package de.mkammerer.raft.util;

import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;

public final class FutureUtil {
    private FutureUtil() {
    }

    public static <T> T getUnchecked(Future<T> result) {
        try {
            return result.get();
        } catch (InterruptedException | ExecutionException e) {
            throw new RuntimeException(e);
        }
    }
}
