package cws.k8s.scheduler.model;

import lombok.AccessLevel;
import lombok.NoArgsConstructor;
import lombok.ToString;

@ToString
@NoArgsConstructor(access = AccessLevel.NONE, force = true)
public class InputParam<T> {

    public final String name;
    public final T value;

    /**
     * Only for testing
     */
    public InputParam( String name, T value ) {
        this.name = name;
        this.value = value;
    }

}
