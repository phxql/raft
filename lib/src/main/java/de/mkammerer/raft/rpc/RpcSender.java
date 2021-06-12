package de.mkammerer.raft.rpc;

import de.mkammerer.raft.NodeId;

import java.util.concurrent.Future;

public interface RpcSender {
    Future<AppendEntriesRpc.Result> sendAppendEntriesRpc(NodeId nodeId, AppendEntriesRpc rpc);

    Future<RequestVoteRpc.Result> sendRequestVoteRpc(NodeId nodeId, RequestVoteRpc rpc);
}
