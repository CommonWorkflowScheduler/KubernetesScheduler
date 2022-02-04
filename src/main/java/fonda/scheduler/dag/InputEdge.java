package fonda.scheduler.dag;

import lombok.Getter;
import lombok.ToString;

@Getter
@ToString
public class InputEdge {

    private final String label;
    private final int from;
    private final int to;

    private InputEdge() {
        this.label = null;
        this.from = -1;
        this.to = -1;
    }

}
