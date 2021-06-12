package de.mkammerer.raft.state;

import de.mkammerer.raft.util.Assert;
import lombok.Value;

import java.util.ArrayList;
import java.util.List;

@Value
public class Log {
    List<Entry> entries;

    public static Log empty() {
        return new Log(new ArrayList<>());
    }

    /**
     * Compares this log against a received log.
     *
     * @param lastLogIndex received last log index
     * @param lastLogTerm  received last log term
     * @return {@link CompareResult#OLDER} if this instance is older than the received one.
     * {@link CompareResult#SAME} if this instance is the same as the received one.
     * {@link CompareResult#NEWER} if this instance is newer than the received one.
     */
    public CompareResult compare(long lastLogIndex, long lastLogTerm) {
        long ourIndex = getLastIndex();
        long ourLastLogTerm = getLastTerm();

        // Raft determines which of two logs is more up-to-date by comparing the index and term of the last entries in
        // the logs. If the logs have last entries with different terms, then the log with the later term is more
        // up-to-date. If the logs end with the same term, then whichever log is longer is more up-to-date.

        if (ourLastLogTerm > lastLogTerm) {
            return CompareResult.NEWER;
        }
        if (ourLastLogTerm < lastLogTerm) {
            return CompareResult.OLDER;
        }

        if (ourIndex > lastLogIndex) {
            return CompareResult.NEWER;
        }
        if (ourIndex < lastLogIndex) {
            return CompareResult.OLDER;
        }

        return CompareResult.SAME;
    }

    /**
     * Checks if log contains an entry at prevLogIndex whose term matches prevLogTerm
     *
     * @param prevLogIndex prevLogIndex, 1-based
     * @param prevLogTerm  prevLogTerm
     * @return true if log contains entry, false otherwise
     */
    public boolean hasEntry(long prevLogIndex, long prevLogTerm) {
        if (!hasIndex(prevLogIndex)) {
            return false;
        }

        return getAtIndex(prevLogIndex).getReceivedTerm() == prevLogTerm;
    }

    public boolean hasIndex(long index) {
        return index <= entries.size();
    }

    public void removeConflicting(long index, List<Entry> entries) {
        long currentIndex = index;
        for (Entry entry : entries) {
            if (!hasIndex(currentIndex)) {
                return;
            }

            if (getAtIndex(currentIndex).getReceivedTerm() != entry.getReceivedTerm()) {
                // if an existing entry conflicts with a new one (same index but different terms), delete the existing entry and all that follow it
                removeFromIndex(currentIndex);
                return;
            }
            currentIndex++;
        }
    }

    /**
     * Removes all entries at the given index and all that follow it.
     *
     * @param index index
     */
    private void removeFromIndex(long index) {
        while (hasIndex(index)) {
            entries.remove(entries.size() - 1);
        }
    }


    public Entry getAtIndex(long index) {
        return entries.get((int) (index - 1));
    }

    public void append(long index, List<Entry> entries) {
        long currentIndex = index;

        for (Entry entry : entries) {
            if (hasIndex(currentIndex)) {
                Assert.that(getAtIndex(currentIndex).getReceivedTerm() == entry.getReceivedTerm(), "getAtIndex(currentIndex).getReceivedTerm() == entry.getReceivedTerm()");
            } else {
                this.entries.add(entry);
            }

            currentIndex++;
        }
    }

    public void append(Entry entry) {
        entries.add(entry);
    }

    public long getLastIndex() {
        return entries.size();
    }

    public long getTermAt(long index) {
        return getAtIndex(index).getReceivedTerm();
    }

    public long getLastTerm() {
        if (entries.isEmpty()) {
            return 0;
        }

        return entries.get(entries.size() - 1).getReceivedTerm();
    }

    public List<Entry> getEntriesFrom(long nextIndex) {
        return entries.subList((int) (nextIndex - 1), entries.size());
    }

    public enum CompareResult {
        OLDER,
        SAME,
        NEWER;
    }

    @Value
    public static class Entry {
        /**
         * command for state machine.
         */
        String data;
        /**
         * term when entrywas received by leader.
         */
        long receivedTerm;
    }
}
