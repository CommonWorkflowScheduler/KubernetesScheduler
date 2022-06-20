package fonda.scheduler.model;

import lombok.Getter;
import lombok.ToString;

@Getter
@ToString
public class OutLabel {

    private final String label;
    private final double weight;

    private OutLabel() {
        this.label = null;
        this.weight = -1;
    }

    public OutLabel(String label, double weight) {
        this.label = label;
        this.weight = weight;
    }
}
