package cws.k8s.scheduler.model;

import lombok.Getter;
import lombok.RequiredArgsConstructor;
import lombok.ToString;

@Getter
@ToString
@RequiredArgsConstructor
public class OutLabel {

    private final String label;
    private final double weight;

    private OutLabel() {
        this(null,-1 );
    }

}
