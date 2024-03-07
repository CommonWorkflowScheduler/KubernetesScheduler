package cws.k8s.scheduler.model;

import lombok.AccessLevel;
import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.ToString;

@ToString
@Getter
@NoArgsConstructor(access = AccessLevel.PRIVATE, force = true)
public class TaskMetrics {

    private final long ramRequest;
    private final long peakVmem;
    private final long peakRss;
    private final long realtime;

}
