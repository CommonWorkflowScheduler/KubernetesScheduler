package cws.k8s.scheduler.model;

import lombok.*;

@ToString
@Getter
@NoArgsConstructor(access = AccessLevel.PRIVATE, force = true)
@RequiredArgsConstructor
public class TaskMetrics {

    private final long ramRequest;
    private final long peakVmem;
    private final long peakRss;
    private final long realtime;

}
