package cws.k8s.scheduler.dag;

import java.util.HashSet;
import java.util.Set;

public class Origin extends NotProcess {

    /**
     * Only public for tests
     */
    public Origin(String label, int uid) {
        super(label, uid);
    }

    @Override
    public Type getType() {
        return Type.ORIGIN;
    }

    @Override
    public void addInbound(Edge e) {
        throw new IllegalStateException("Cannot add an Edge(uid: " + e.getUid() + "; "  + e.getFrom().getUid() + " -> " + e.getTo().getUid() + ") inbound to an Origin (uid: " + getUid() + ")");
    }

    public Set<Process> getAncestors() {
        return new HashSet<>();
    }

}
