package cws.k8s.scheduler.model;

import cws.k8s.scheduler.util.NodeTaskAlignment;
import lombok.Getter;
import lombok.RequiredArgsConstructor;
import lombok.Setter;

import java.util.List;

@Setter
@Getter
@RequiredArgsConstructor
public class ScheduleObject {

    private final List<NodeTaskAlignment> taskAlignments;
    private boolean checkStillPossible = false;
    private boolean stopSubmitIfOneFails = false;

}
