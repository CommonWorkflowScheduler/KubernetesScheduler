package fonda.scheduler.scheduler;

import fonda.scheduler.client.KubernetesClient;
import fonda.scheduler.model.*;
import fonda.scheduler.model.location.NodeLocation;
import fonda.scheduler.model.location.hierachy.NoAlignmentFoundException;
import fonda.scheduler.model.taskinputs.TaskInputs;
import fonda.scheduler.scheduler.filealignment.InputAlignment;
import fonda.scheduler.util.FileAlignment;
import fonda.scheduler.util.NodeTaskAlignment;
import fonda.scheduler.util.NodeTaskFilesAlignment;
import lombok.extern.slf4j.Slf4j;

import java.util.*;

@Slf4j
public class RandomLAScheduler extends SchedulerWithDaemonSet {

    private final InputAlignment inputAlignment;
    private final Random random = new Random();

    public RandomLAScheduler(
            String name,
            KubernetesClient client,
            String namespace,
            SchedulerConfig config,
            InputAlignment inputAlignment
    ) {
        super(name, client, namespace, config);
        this.inputAlignment = inputAlignment;
    }

    private Optional<NodeWithAlloc> selectNode( Set<NodeWithAlloc> matchingNodes, Task task ){
        return matchingNodes.isEmpty()
                ? Optional.empty()
                : Optional.of( new LinkedList<>(matchingNodes).get(random.nextInt(matchingNodes.size())));
    }

    @Override
    public ScheduleObject getTaskNodeAlignment(
            final List<Task> unscheduledTasks,
            final Map<NodeWithAlloc, Requirements> availableByNode
    ){
        List<NodeTaskAlignment> alignment = new LinkedList<>();

        int index = 0;
        for ( final Task task : unscheduledTasks ) {
            index++;
            final PodWithAge pod = task.getPod();
            log.info("Pod: " + pod.getName() + " Requested Resources: " + pod.getRequest());

            final Set<NodeWithAlloc> matchingNodes = getMatchingNodesForTask(availableByNode,task);
            if( !matchingNodes.isEmpty() ) {

                final TaskInputs inputsOfTask;
                try {
                    inputsOfTask = getInputsOfTask(task);
                } catch (NoAlignmentFoundException e) {
                    continue;
                }

                if( inputsOfTask == null ) {
                    log.info( "No node where the pod can start, pod: {}", pod.getName() );
                    continue;
                }

                filterNotMatchingNodesForTask( matchingNodes, inputsOfTask );

                if( matchingNodes.isEmpty() ) {
                    log.info( "No node which fulfills all requirements {}", pod.getName() );
                    continue;
                }

                Optional<NodeWithAlloc> node = selectNode( matchingNodes, task );
                if( node.isPresent() ) {
                    final FileAlignment fileAlignment = inputAlignment.getInputAlignment( task, inputsOfTask, node.get() );
                    alignment.add(new NodeTaskFilesAlignment(node.get(), task, fileAlignment));
                    availableByNode.get(node.get()).subFromThis(pod.getRequest());
                    log.info("--> " + node.get().getName());
                    if ( traceEnabled ){
                        task.getTraceRecord().setSchedulerPlaceInQueue( index );
                        task.getTraceRecord().setSchedulerLocationCount(
                                inputsOfTask.getFiles()
                                        .parallelStream()
                                        .mapToInt( x -> x.locations.size() )
                                        .sum()
                        );
                    }
                }

            } else {
                log.info( "No node with enough resources for {}", pod.getName() );
            }
        }
        final ScheduleObject scheduleObject = new ScheduleObject(alignment);
        scheduleObject.setCheckStillPossible( true );
        return scheduleObject;
    }

}
