package fonda.scheduler.util.copying;

import fonda.scheduler.model.Task;
import fonda.scheduler.model.location.Location;
import lombok.Getter;
import lombok.RequiredArgsConstructor;

@Getter
@RequiredArgsConstructor
public class CopySource {

    private final Task task;
    private final Location location;

}
