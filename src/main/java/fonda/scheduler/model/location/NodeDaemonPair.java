package fonda.scheduler.model.location;

public class NodeDaemonPair {

    public final boolean sameAsEngine;
    public final String node;
    public final String daemon;

    public NodeDaemonPair( String node, String daemon, boolean sameAsEngine ) {
        this.sameAsEngine = sameAsEngine;
        this.node = node;
        this.daemon = daemon;
    }
}
