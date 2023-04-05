package cws.k8s.scheduler.model;

import lombok.AccessLevel;
import lombok.NoArgsConstructor;
import lombok.RequiredArgsConstructor;
import lombok.ToString;

@ToString
@NoArgsConstructor(access = AccessLevel.NONE, force = true)
@RequiredArgsConstructor
public class InputParam<T> {

    public final String name;
    public final T value;

}
