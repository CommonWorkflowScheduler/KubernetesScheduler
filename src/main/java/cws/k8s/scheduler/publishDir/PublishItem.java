package cws.k8s.scheduler.publishDir;

import lombok.*;

import java.nio.file.Path;

@ToString
@RequiredArgsConstructor
@NoArgsConstructor( access = AccessLevel.PRIVATE, force = true )
@Getter
public class PublishItem {

    private final Path source;
    private final Path destination;
    private final PublishMode mode;

}
