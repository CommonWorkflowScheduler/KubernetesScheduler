package fonda.scheduler.util;

import lombok.Getter;
import lombok.ToString;

@Getter
@ToString
public class Tuple <S,T> {

    private final S a;
    private final T b;

    public Tuple( S a, T b ) {
        this.a = a;
        this.b = b;
    }
}
