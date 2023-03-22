package cws.k8s.scheduler.util.copying;

import cws.k8s.scheduler.model.Task;
import cws.k8s.scheduler.model.location.Location;
import lombok.Getter;
import lombok.RequiredArgsConstructor;

@Getter
@RequiredArgsConstructor
public class CopySource {

    private final Task task;
    private final Location location;

}
