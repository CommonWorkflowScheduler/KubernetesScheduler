package cws.k8s.scheduler.dag;

import java.util.HashSet;
import java.util.Set;

public class Origin extends NotProcess {

    /**
     * Only public for tests
     * @param label
     * @param uid
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
        throw new IllegalStateException("Cannot add an inbound to an Origin");
    }

    public Set<Process> getAncestors() {
        return new HashSet<>();
    }

}
