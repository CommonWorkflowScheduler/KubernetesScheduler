package fonda.scheduler.dag;

import lombok.Getter;
import lombok.ToString;

@Getter
@ToString
public class InputEdge {

    private final String label;
    private final int from;
    private final int to;

    @SuppressWarnings("unused")
    private InputEdge() {
        this.label = null;
        this.from = -1;
        this.to = -1;
    }

    /**
     * Just for testing
     * @param from
     * @param to
     */
    public InputEdge(int from, int to) {
        label = null;
        this.from = from;
        this.to = to;
    }
}
