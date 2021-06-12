package de.mkammerer.raft;

import de.mkammerer.raft.rpc.LocalRpcSender;
import de.mkammerer.raft.rpc.RpcSender;
import lombok.extern.slf4j.Slf4j;
import org.checkerframework.checker.nullness.qual.Nullable;

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;

@Slf4j
public class Main {
    public static final int NUMBER_OF_NODES = 3;

    public static void main(String[] args) {
        log.info("Started");

        ScheduledExecutorService scheduler = Executors.newScheduledThreadPool(NUMBER_OF_NODES);
        Clock clock = System::nanoTime;
        Random random = new Random();

        Map<NodeId, Node> nodes = new HashMap<>(NUMBER_OF_NODES);

        for (int i = 0; i < NUMBER_OF_NODES; i++) {
            NodeId nodeId = NodeId.of(i);
            Node node = new Node(nodeId, createOtherNodeIds(i), random, scheduler, clock);
            nodes.put(nodeId, node);
        }

        RpcSender rpcSender = new LocalRpcSender(nodes);

        for (Node node : nodes.values()) {
            node.start(rpcSender);
        }

        scheduler.execute(() -> sendLoop(nodes.values()));
    }

    private static void sendLoop(Collection<Node> nodes) {
        try {
            int current = 0;
            while (true) {
                Thread.sleep(1000);
                Node leader = findLeader(nodes);
                if (leader != null) {
                    log.info("Sending '{}' to {}", current, leader.getId());
                    leader.send(Integer.toString(current));
                    current++;
                }
            }
        } catch (Exception t) {
            log.error("", t);
            throw new RuntimeException(t);
        }
    }

    @Nullable
    private static Node findLeader(Collection<Node> nodes) {
        for (Node node : nodes) {
            if (node.isLeader()) {
                return node;
            }
        }

        return null;
    }

    private static List<NodeId> createOtherNodeIds(int ownIndex) {
        List<NodeId> otherNodes = new ArrayList<>(NUMBER_OF_NODES - 1);
        for (int i = 0; i < NUMBER_OF_NODES; i++) {
            if (ownIndex == i) {
                continue;
            }

            otherNodes.add(NodeId.of(i));
        }

        return otherNodes;
    }
}
