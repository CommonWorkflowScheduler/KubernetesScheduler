package cws.k8s.scheduler.util;

import lombok.Getter;
import lombok.RequiredArgsConstructor;
import lombok.ToString;

@Getter
@ToString
@RequiredArgsConstructor
public class Tuple <S,T> {

    private final S a;
    private final T b;

}
