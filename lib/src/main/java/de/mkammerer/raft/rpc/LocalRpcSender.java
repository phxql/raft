package de.mkammerer.raft.rpc;

import de.mkammerer.raft.Node;
import de.mkammerer.raft.NodeId;
import lombok.RequiredArgsConstructor;

import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Future;

@RequiredArgsConstructor
public class LocalRpcSender implements RpcSender {
    private final Map<NodeId, Node> nodes;

    @Override
    public Future<AppendEntriesRpc.Result> sendAppendEntriesRpc(NodeId nodeId, AppendEntriesRpc rpc) {
        return CompletableFuture.completedFuture(nodes.get(nodeId).handleAppendEntriesRpc(rpc));
    }

    @Override
    public Future<RequestVoteRpc.Result> sendRequestVoteRpc(NodeId nodeId, RequestVoteRpc rpc) {
        return CompletableFuture.completedFuture(nodes.get(nodeId).handleRequestVoteRpc(rpc));
    }
}
