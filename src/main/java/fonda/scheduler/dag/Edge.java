package fonda.scheduler.dag;

import lombok.*;

@Getter
@ToString
@EqualsAndHashCode
@RequiredArgsConstructor( access = AccessLevel.PACKAGE )
public class Edge {

    private final int uid;
    private final String label;
    private final Vertex from;
    private final Vertex to;

    Edge(int uid, Vertex from, Vertex to) {
        this(uid, null, from, to);
    }

}
