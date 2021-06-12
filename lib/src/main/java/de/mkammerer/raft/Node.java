package de.mkammerer.raft;

import de.mkammerer.raft.rpc.AppendEntriesRpc;
import de.mkammerer.raft.rpc.RequestVoteRpc;
import de.mkammerer.raft.rpc.RpcSender;
import de.mkammerer.raft.state.LeaderState;
import de.mkammerer.raft.state.Log;
import de.mkammerer.raft.state.PersistentState;
import de.mkammerer.raft.state.VolatileState;
import de.mkammerer.raft.util.FutureUtil;
import de.mkammerer.raft.util.LoggingRunnable;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.checkerframework.checker.nullness.qual.Nullable;

import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.Future;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

@RequiredArgsConstructor
@Slf4j
public class Node {
    private VolatileState volatileState = VolatileState.create();
    @Nullable
    private LeaderState leaderState = null;
    private PersistentState persistentState = PersistentState.create();

    private final NodeId nodeId;
    private final List<NodeId> otherNodes;
    private RpcSender rpcSender;
    private final Random random;
    private final ScheduledExecutorService scheduler;
    private final Clock clock;
    private int majority;

    public synchronized RequestVoteRpc.Result handleRequestVoteRpc(RequestVoteRpc rpc) {
        log.trace("[{}] handleRequestVoteRpc({})", nodeId, rpc);

        // If RPC request or response contains term T > currentTerm: set currentTerm = T, convert to follower
        if (rpc.getTerm() > persistentState.getCurrentTerm()) {
            persistentState.setCurrentTerm(rpc.getTerm());
            convertToFollower();
        }

        // Reply false if term < currentTerm
        if (rpc.getTerm() < persistentState.getCurrentTerm()) {
            log.debug("[{}] Denied vote for {}", nodeId, rpc.getCandidateId());
            return RequestVoteRpc.Result.no(persistentState.getCurrentTerm());
        }

        //  votedFor is null or candidateId, and candidate’s log is at least as up-to-date as receiver’s log, grant vote
        if (persistentState.getVotedFor() == null || persistentState.getVotedFor().equals(rpc.getCandidateId())) {
            Log.CompareResult logCompare = persistentState.getLog().compare(rpc.getLastLogIndex(), rpc.getLastLogTerm());

            if (logCompare == Log.CompareResult.SAME || logCompare == Log.CompareResult.OLDER) {
                receivedHeartBeat();

                log.debug("[{}] Granted vote to {}", nodeId, rpc.getCandidateId());
                persistentState.setVotedFor(rpc.getCandidateId());
                return RequestVoteRpc.Result.yes(persistentState.getCurrentTerm());
            }
        }

        log.debug("[{}] Denied vote for {}", nodeId, rpc.getCandidateId());
        return RequestVoteRpc.Result.no(persistentState.getCurrentTerm());
    }

    public synchronized AppendEntriesRpc.Result handleAppendEntriesRpc(AppendEntriesRpc rpc) {
        log.trace("[{}] handleAppendEntriesRpc({})", nodeId, rpc);

        // If RPC request or response contains term T > currentTerm: set currentTerm = T, convert to follower
        if (rpc.getTerm() > persistentState.getCurrentTerm()) {
            persistentState.setCurrentTerm(rpc.getTerm());
            convertToFollower();
        }

        // reply false if term < currentTerm
        if (rpc.getTerm() < persistentState.getCurrentTerm()) {
            log.debug("[{}] Append entries denied, old term", rpc);
            return AppendEntriesRpc.Result.fail(persistentState.getCurrentTerm());
        }

        // Check for heartbeat
        if (rpc.getPrevLogIndex() == 0 && rpc.getPrevLogTerm() == 0 && rpc.getEntries().isEmpty()) {
            log.trace("[{}] Received heartbeat", nodeId);
            return AppendEntriesRpc.Result.ok(persistentState.getCurrentTerm());
        }

        // reply false if log doesn’t contain an entry at prevLogIndex whose term matches prevLogTerm
        if (rpc.getPrevLogIndex() > 0 && !persistentState.getLog().hasEntry(rpc.getPrevLogIndex(), rpc.getPrevLogTerm())) {
            log.debug("[{}] Append entries denied, prev entry term doesn't match", nodeId);
            return AppendEntriesRpc.Result.fail(persistentState.getCurrentTerm());
        }

        // if an existing entry conflicts with a new one (same index but different terms), delete the existing entry and all that follow it
        persistentState.getLog().removeConflicting(rpc.getPrevLogIndex() + 1, rpc.getEntries());
        // append any new entries not already in the log
        persistentState.getLog().append(rpc.getPrevLogIndex() + 1, rpc.getEntries());

        // if leaderCommit > commitIndex, set commitIndex = min(leaderCommit, index of last new entry)
        if (rpc.getLeaderCommit() > volatileState.getCommitIndex()) {
            volatileState.setCommitIndex(Math.min(rpc.getLeaderCommit(), persistentState.getLog().getLastIndex()));
            commitLog();
        }

        // If AppendEntries RPC received from new leader: convert to follower
        if (volatileState.getRole() != VolatileState.Role.FOLLOWER) {
            convertToFollower();
        }

        receivedHeartBeat();

        log.debug("[{}] Added {} to log", nodeId, rpc.getEntries());

        return AppendEntriesRpc.Result.ok(persistentState.getCurrentTerm());
    }

    public void start(RpcSender rpcSender) {
        this.rpcSender = rpcSender;

        majority = ((otherNodes.size() + 1) / 2) + 1;
        Duration electionTimeout = random.getElectionTimeout();
        scheduler.schedule(new LoggingRunnable(() -> this.checkHeartBeat(electionTimeout)), electionTimeout.toMillis(), TimeUnit.MILLISECONDS);
    }

    public void send(String command) {
        if (volatileState.getRole() != VolatileState.Role.LEADER) {
            throw new IllegalStateException("Can only send commands to leader");
        }

        // if command received from client: append entry to local log,respond after entry applied to state machine
        persistentState.getLog().append(new Log.Entry(command, persistentState.getCurrentTerm()));

        long lastIndex = persistentState.getLog().getLastIndex();

        // if last log index ≥ nextIndex for a follower: sendAppendEntries RPC with log entries starting at nextIndex
        for (NodeId otherNode : otherNodes) {
            long nextIndex = leaderState.getNextIndex().get(otherNode);
            while (true) {
                if (lastIndex >= nextIndex) {
                    long prevIndex = nextIndex - 1;
                    long prevTerm = prevIndex == 0 ? 0 : persistentState.getLog().getTermAt(prevIndex);

                    List<Log.Entry> entriesToSend = persistentState.getLog().getEntriesFrom(nextIndex);
                    log.debug("[{}] Sending entries from index {} to {}: {}", nodeId, nextIndex, otherNode, entriesToSend);

                    Future<AppendEntriesRpc.Result> future = rpcSender.sendAppendEntriesRpc(otherNode, new AppendEntriesRpc(persistentState.getCurrentTerm(), otherNode, prevIndex, prevTerm, entriesToSend, volatileState.getCommitIndex()));
                    AppendEntriesRpc.Result result = FutureUtil.getUnchecked(future);

                    // If RPC request or response contains term T > currentTerm: set currentTerm = T, convert to follower
                    if (result.getTerm() > persistentState.getCurrentTerm()) {
                        persistentState.setCurrentTerm(result.getTerm());
                        convertToFollower();
                        return;
                    }

                    if (result.isSuccess()) {
                        log.debug("[{}] Updated state on {}", nodeId, otherNode);
                        // if successful: update nextIndex and matchIndex for follower
                        leaderState.getNextIndex().put(otherNode, lastIndex + 1);
                        leaderState.getMatchIndex().put(otherNode, lastIndex);
                        break;
                    } else {
                        log.debug("[{}] Updating state on {} failed, retrying", nodeId, otherNode);
                        // if AppendEntries fails because of log inconsistency: decrement nextIndex and retry
                        nextIndex--;
                    }
                }
            }
        }

        // If there exists an N such that N > commitIndex, a majority of matchIndex[i] ≥ N, and log[N].term == currentTerm: set commitIndex = N
        for (long n = volatileState.getCommitIndex() + 1; n <= persistentState.getLog().getLastIndex(); n++) {
            int replicated = 1; // We have this data already
            for (NodeId otherNode : otherNodes) {
                long matchIndex = leaderState.getMatchIndex().get(otherNode);
                if (matchIndex >= n) {
                    replicated++;
                }
            }

            if (replicated >= majority && persistentState.getLog().getTermAt(n) == persistentState.getCurrentTerm()) {
                volatileState.setCommitIndex(n);
                commitLog();
            }
        }
    }

    private void commitLog() {
        // If commitIndex > lastApplied: increment lastApplied, apply log[lastApplied] to state machine
        if (volatileState.getCommitIndex() > volatileState.getLastApplied()) {
            volatileState.incrementLastApplied();
            log.info("[{}] Log entry {} is now committed. Entries: {}", nodeId, persistentState.getLog().getAtIndex(volatileState.getLastApplied()), persistentState.getLog().getEntries());
        }
    }

    private void checkHeartBeat(Duration electionTimeout) {
        log.trace("[{}] checkHeartBeat()", nodeId);

        if (volatileState.getRole() == VolatileState.Role.LEADER) {
            // Leaders don't have timeouts
            return;
        }

        if (volatileState.isHeartBeatExpired(electionTimeout, clock.nanoTime())) {
            log.debug("[{}] Timeout, converting to candidate", nodeId);
            // If election timeout elapses without receiving AppendEntriesRPC from current leader or granting vote to candidate: convert to candidate
            convertToCandidate();
        }

        Duration newElectionTimeout = random.getElectionTimeout();
        scheduler.schedule(new LoggingRunnable(() -> checkHeartBeat(newElectionTimeout)), newElectionTimeout.toMillis(), TimeUnit.MILLISECONDS);
    }

    private void convertToCandidate() {
        // On conversion to candidate, start election:
        log.info("[{}] Converting to candidate", nodeId);
        volatileState.setRole(VolatileState.Role.CANDIDATE);
        leaderState = null;

        startElection();
    }

    private void convertToFollower() {
        log.info("[{}] Converting to follower", nodeId);
        volatileState.setRole(VolatileState.Role.FOLLOWER);
        leaderState = null;
    }

    private void startElection() {
        log.info("[{}] Starting election", nodeId);

        // increment currentTerm
        persistentState.incrementCurrentTerm();

        // Vote for self
        persistentState.setVotedFor(nodeId);

        // Reset election timer
        // TODO

        List<Future<RequestVoteRpc.Result>> results = new ArrayList<>(otherNodes.size());
        // Send RequestVote RPCs to all other servers
        for (NodeId otherNode : otherNodes) {
            results.add(
                    rpcSender.sendRequestVoteRpc(otherNode, new RequestVoteRpc(persistentState.getCurrentTerm(), nodeId, persistentState.getLog().getLastIndex(), persistentState.getLog().getLastTerm()))
            );
        }

        handleVoteResults(results);
    }

    private void handleVoteResults(List<Future<RequestVoteRpc.Result>> futures) {
        int votes = 1; // we voted for ourself

        for (Future<RequestVoteRpc.Result> future : futures) {
            RequestVoteRpc.Result result = FutureUtil.getUnchecked(future);
            // If RPC request or response contains term T > currentTerm: set currentTerm = T, convert to follower
            if (result.getTerm() > persistentState.getCurrentTerm()) {
                persistentState.setCurrentTerm(result.getTerm());
                convertToFollower();
                return;
            }

            if (result.isVoteGranted()) {
                votes++;

                // If votes received from majority of servers: become leader
                if (votes >= majority) {
                    convertToLeader();
                    return;
                }
            }
        }

        log.debug("[{}] Unable to get majority of votes in term {}", nodeId, persistentState.getCurrentTerm());
    }

    private void convertToLeader() {
        log.info("[{}] Converting to leader, term {}", nodeId, persistentState.getCurrentTerm());

        leaderState = LeaderState.create(otherNodes, persistentState.getLog().getLastIndex());
        volatileState.setRole(VolatileState.Role.LEADER);

        // Upon election: send initial empty AppendEntries RPCs (heartbeat) to each server; repeat during idle periods to prevent election timeouts
        sendHeartbeat();
    }

    private void receivedHeartBeat() {
        log.trace("[{}] Received heartbeat", nodeId);
        volatileState.setLastHeartBeat(clock.nanoTime());
    }

    private void sendHeartbeat() {
        if (volatileState.getRole() != VolatileState.Role.LEADER) {
            return;
        }

        // Upon election: send initial empty AppendEntries RPCs (heartbeat) to each server; repeat during idle periods to prevent election timeouts
        for (NodeId otherNode : otherNodes) {
            rpcSender.sendAppendEntriesRpc(otherNode, new AppendEntriesRpc(persistentState.getCurrentTerm(), nodeId, 0, 0, List.of(), volatileState.getCommitIndex()));
        }

        scheduler.schedule(new LoggingRunnable(this::sendHeartbeat), Duration.ofMillis(100).toMillis(), TimeUnit.MILLISECONDS);
    }

    public boolean isLeader() {
        return volatileState.getRole() == VolatileState.Role.LEADER;
    }

    public NodeId getId() {
        return nodeId;
    }
}
