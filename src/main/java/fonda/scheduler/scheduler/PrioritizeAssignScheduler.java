package fonda.scheduler.scheduler;

import fonda.scheduler.client.Informable;
import fonda.scheduler.client.KubernetesClient;
import fonda.scheduler.model.*;
import fonda.scheduler.scheduler.nodeassign.NodeAssign;
import fonda.scheduler.scheduler.prioritize.Prioritize;
import fonda.scheduler.util.NodeTaskAlignment;
import lombok.extern.slf4j.Slf4j;

import java.util.*;

@Slf4j
public class PrioritizeAssignScheduler extends Scheduler {

    private final Prioritize prioritize;
    private final NodeAssign nodeAssigner;

    public PrioritizeAssignScheduler( String execution,
                                      KubernetesClient client,
                                      String namespace,
                                      SchedulerConfig config,
                                      Prioritize prioritize,
                                      NodeAssign nodeAssigner ) {
        super(execution, client, namespace, config);
        this.prioritize = prioritize;
        this.nodeAssigner = nodeAssigner;
        nodeAssigner.registerScheduler( this );
        if ( nodeAssigner instanceof Informable ){
            client.addInformable( (Informable) nodeAssigner );
        }
    }

    @Override
    public void close() {
        super.close();
        if ( nodeAssigner instanceof Informable ){
            client.removeInformable( (Informable) nodeAssigner );
        }
    }

    @Override
    public ScheduleObject getTaskNodeAlignment(
            final List<Task> unscheduledTasks,
            final Map<NodeWithAlloc, Requirements> availableByNode
    ){

        prioritize.sortTasks( unscheduledTasks );
        List<NodeTaskAlignment> alignment = nodeAssigner.getTaskNodeAlignment(unscheduledTasks, availableByNode);

        final ScheduleObject scheduleObject = new ScheduleObject(alignment);
        scheduleObject.setCheckStillPossible( false );
        return scheduleObject;
    }

}
