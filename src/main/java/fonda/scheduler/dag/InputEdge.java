package fonda.scheduler.dag;

import lombok.*;

@Getter
@ToString
@RequiredArgsConstructor
@NoArgsConstructor(force = true, access = AccessLevel.PRIVATE)
public class InputEdge {

    private final int uid;
    private final String label = null;
    private final int from;
    private final int to;

}
