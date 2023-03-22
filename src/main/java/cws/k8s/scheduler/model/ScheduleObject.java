package cws.k8s.scheduler.model;

import cws.k8s.scheduler.util.NodeTaskAlignment;
import lombok.Getter;
import lombok.Setter;

import java.util.List;

@Setter
@Getter
public class ScheduleObject {

    private final List<NodeTaskAlignment> taskAlignments;
    private boolean checkStillPossible = false;
    private boolean stopSubmitIfOneFails = false;

    public ScheduleObject( List<NodeTaskAlignment> taskAlignments ) {
        this.taskAlignments = taskAlignments;
    }
    
}
