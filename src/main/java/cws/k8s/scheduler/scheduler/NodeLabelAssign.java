package cws.k8s.scheduler.scheduler;

import cws.k8s.scheduler.model.*;
import cws.k8s.scheduler.client.KubernetesClient;
import cws.k8s.scheduler.util.NodeTaskAlignment;
import lombok.extern.slf4j.Slf4j;

import org.apache.commons.lang3.tuple.Pair;

import java.util.*;

@Slf4j
public class NodeLabelAssign extends Scheduler {

    public Map<String, String> nodelabel;

    public NodeLabelAssign( String execution,
                            KubernetesClient client,
                            String namespace,
                            SchedulerConfig config,
                            final Map<String, String> nodelabel
                            ) {
        super(execution, client, namespace, config);
        this.nodelabel = nodelabel;
    }

    @Override
    public ScheduleObject getTaskNodeAlignment(
            final List<Task> unscheduledTasks,
            final Map<NodeWithAlloc, Requirements> availableByNode
            ){
        final ArrayList<Map.Entry<NodeWithAlloc, Requirements>> entries = new ArrayList<>( availableByNode.entrySet() );
        List<NodeTaskAlignment> alignment = new LinkedList<>();

        long start = System.currentTimeMillis();
        if ( traceEnabled ) {
            int index = 1;
            for ( Task unscheduledTask : unscheduledTasks ) {
                unscheduledTask.getTraceRecord().setSchedulerPlaceInQueue( index++ );
            }
        }

        for ( Task unscheduledTask : unscheduledTasks ) {


            final String taskLabel = unscheduledTask.getProcess().getLabel();
            System.out.println("Task Label: " + taskLabel);

            if(nodelabel.containsKey(taskLabel)){
                String nodeName = nodelabel.get(taskLabel);
                System.out.println("Node Name: " + nodeName);
                // int resource = nodeResourcePair.getRight();  // add resource cap 

                for ( Map.Entry<NodeWithAlloc, Requirements> e : entries ) {
                    final NodeWithAlloc node = e.getKey();
                   
                    if(nodeName == node.getName()){
                        alignment.add( new NodeTaskAlignment( node, unscheduledTask ) );
                    }
                }
            }
        }

        long timeDelta = System.currentTimeMillis() - start;
        for ( Task unscheduledTask : unscheduledTasks ) {
            unscheduledTask.getTraceRecord().setSchedulerTimeToSchedule( (int) timeDelta );
        }

        final ScheduleObject scheduleObject = new ScheduleObject(alignment);
        scheduleObject.setCheckStillPossible( false );
        return scheduleObject;
    }
}