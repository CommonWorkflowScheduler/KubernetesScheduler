package fonda.scheduler.dag;

import lombok.Getter;
import lombok.ToString;

@Getter
@ToString
public class Edge {

    private final String label;
    private final Vertex from;
    private final Vertex to;

    public Edge(String label, Vertex from, Vertex to) {
        this.label = label;
        this.from = from;
        this.to = to;
    }
}
