package fonda.scheduler.model;

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
     * @param name
     * @param value
     */
    public InputParam( String name, T value ) {
        this.name = name;
        this.value = value;
    }

}
