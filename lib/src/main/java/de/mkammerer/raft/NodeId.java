package de.mkammerer.raft;

import lombok.Value;

@Value(staticConstructor = "of")
public class NodeId {
    long id;

    @Override
    public String toString() {
        return Long.toString(id);
    }
}
