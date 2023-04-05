package cws.k8s.scheduler.dag;

import lombok.Getter;
import lombok.ToString;

@Getter
@ToString
public class InputEdge {

    private final int uid;
    private final String label;
    private final int from;
    private final int to;

    @SuppressWarnings("unused")
    private InputEdge() {
        this(-1, -1, -1);
    }

    /**
     * Just for testing
     */
    public InputEdge( int uid, int from, int to) {
        this.uid = uid;
        label = null;
        this.from = from;
        this.to = to;
    }
}
