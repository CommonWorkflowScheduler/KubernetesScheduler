package cws.k8s.scheduler.dag;

import lombok.*;

@Getter
@ToString
@RequiredArgsConstructor
@NoArgsConstructor(force = true, access = AccessLevel.PRIVATE)
public class InputEdge {

    private final int uid;
    private final String label;
    private final int from;
    private final int to;

}
