package cws.k8s.scheduler.scheduler;

import cws.k8s.scheduler.model.*;
import cws.k8s.scheduler.scheduler.prioritize.Prioritize;
import cws.k8s.scheduler.scheduler.prioritize.RankMaxPrioritize;
import cws.k8s.scheduler.client.Informable;
import cws.k8s.scheduler.client.KubernetesClient;
import cws.k8s.scheduler.scheduler.nodeassign.LabelAssign;
import cws.k8s.scheduler.scheduler.nodeassign.NodeAssign;
import cws.k8s.scheduler.scheduler.nodeassign.FairAssign;
import cws.k8s.scheduler.util.NodeTaskAlignment;
import lombok.extern.slf4j.Slf4j;

import java.util.*;
import java.util.stream.Collector;
import java.util.stream.Collectors;

@Slf4j
public class NodeLabelAssign extends Scheduler {

    private final Prioritize prioritize;
    private final NodeAssign nodeAssigner;
    private final NodeAssign nodeLabelAssigner;

    public NodeLabelAssign(String execution, KubernetesClient client, String namespace, SchedulerConfig config) {
        this(execution, client, namespace, config, new RankMaxPrioritize(), new LabelAssign(config), new FairAssign());
    }

    public NodeLabelAssign( String execution,
                                      KubernetesClient client,
                                      String namespace,
                                      SchedulerConfig config,
                                      Prioritize prioritize,
                                      NodeAssign nodeLabelAssigner,
                                      NodeAssign nodeAssigner ) {
        super(execution, client, namespace, config);
        this.prioritize = (prioritize != null) ? prioritize : new RankMaxPrioritize();
        this.nodeLabelAssigner = (nodeLabelAssigner != null) ? nodeLabelAssigner : new LabelAssign(config);
        this.nodeAssigner = (nodeAssigner != null) ? nodeAssigner : new FairAssign();
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
        long start = System.currentTimeMillis();
        if ( traceEnabled ) {
            int index = 1;
            for ( Task unscheduledTask : unscheduledTasks ) {
                unscheduledTask.getTraceRecord().setSchedulerPlaceInQueue( index++ );
            }
        }
        prioritize.sortTasks( unscheduledTasks );
        

        unscheduledTasks.stream().map(obj -> obj.getConfig().getName()).forEach(System.out::println);

        // first alignemnt (LabelAssign)
        List<NodeTaskAlignment> alignmentLabelAssign = nodeLabelAssigner.getTaskNodeAlignment(unscheduledTasks, availableByNode);
        List<String> namesList = alignmentLabelAssign.stream().map(obj -> obj.task.getConfig().getName()).collect(Collectors.toList());

        List<Task> filteredTasks = new LinkedList<>();

        for (final Task task : unscheduledTasks) {
            if (!namesList.contains(task.getConfig().getName())) {
                filteredTasks.add(task);
            }
        }

        // second alignemnt (FairAssign)
        List<NodeTaskAlignment> alignment = nodeAssigner.getTaskNodeAlignment(filteredTasks, availableByNode);


        alignmentLabelAssign.addAll(alignment);
        long timeDelta = System.currentTimeMillis() - start;
        for ( Task unscheduledTask : unscheduledTasks ) {
            unscheduledTask.getTraceRecord().setSchedulerTimeToSchedule( (int) timeDelta );
        }

        final ScheduleObject scheduleObject = new ScheduleObject(alignmentLabelAssign);
        scheduleObject.setCheckStillPossible( false );
        return scheduleObject;
    }
}