package cws.k8s.scheduler.model;

import lombok.ToString;

@ToString
public class InputParam<T> {

    public final String name;
    public final T value;

    private InputParam() {
        this.name = null;
        this.value = null;
    }

    /**
     * Only for testing
     * @param name
     * @param value
     */
    public InputParam( String name, T value ) {
        this.name = name;
        this.value = value;
    }

}
