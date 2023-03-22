package cws.k8s.scheduler.rest;

import lombok.AccessLevel;
import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.ToString;

@Getter
@ToString
@NoArgsConstructor( access = AccessLevel.PRIVATE, force = true )
public class PathAttributes {

    private final String path;
    private final long size;
    private final long timestamp;
    private final long locationWrapperID;

}
