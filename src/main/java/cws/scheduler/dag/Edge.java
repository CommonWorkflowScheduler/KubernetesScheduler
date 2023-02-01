package fonda.scheduler.dag;

import lombok.EqualsAndHashCode;
import lombok.Getter;
import lombok.ToString;

@Getter
@ToString
@EqualsAndHashCode
public class Edge {

    private final int uid;
    private final String label;
    private final Vertex from;
    private final Vertex to;

    Edge(int uid, Vertex from, Vertex to) {
        this(uid, null, from, to);
    }

    Edge( int uid, String label, Vertex from, Vertex to) {
        this.uid = uid;
        this.label = label;
        this.from = from;
        this.to = to;
    }
}
