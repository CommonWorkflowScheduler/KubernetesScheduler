package fonda.scheduler.model;

import lombok.Getter;
import lombok.ToString;

@Getter
@ToString
public class OutLabel {

    private final String label;
    private final double weight;

    public OutLabel() {
        this.label = null;
        this.weight = -1;
    }

}
