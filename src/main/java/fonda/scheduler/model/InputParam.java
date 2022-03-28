package fonda.scheduler.model;

import lombok.ToString;

@ToString
public class InputParam<T> {

    public final String name;
    public final T value;

    private InputParam() {
        this.name = null;
        this.value = null;
    }

}
