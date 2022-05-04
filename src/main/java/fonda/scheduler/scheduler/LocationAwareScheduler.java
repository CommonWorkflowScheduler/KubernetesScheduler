package fonda.scheduler.scheduler;

import fonda.scheduler.client.KubernetesClient;
import fonda.scheduler.model.*;
import fonda.scheduler.model.taskinputs.TaskInputs;
import fonda.scheduler.scheduler.data.NodeDataTuple;
import fonda.scheduler.scheduler.data.TaskData;
import fonda.scheduler.scheduler.filealignment.InputAlignment;
import fonda.scheduler.util.FileAlignment;
import fonda.scheduler.util.NodeTaskAlignment;
import fonda.scheduler.util.NodeTaskFilesAlignment;
import fonda.scheduler.util.Tuple;
import lombok.AccessLevel;
import lombok.Getter;
import lombok.extern.slf4j.Slf4j;

import java.util.*;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;

@Slf4j
public class LocationAwareScheduler extends SchedulerWithDaemonSet {

    @Getter(AccessLevel.PACKAGE)
    private final InputAlignment inputAlignment;

    public LocationAwareScheduler (
            String name,
            KubernetesClient client,
            String namespace,
            SchedulerConfig config,
            InputAlignment inputAlignment) {
        super( name, client, namespace, config );
        this.inputAlignment = inputAlignment;
    }



    private NodeTaskFilesAlignment createNodeAlignment (
            final TaskData taskData,
            final Map<NodeWithAlloc, Requirements> availableByNode,
            AtomicInteger index
    ) {
        long startTime = System.nanoTime();
        log.info( "Task: {} has a value of: {}", taskData.getTask().getConfig().getHash(), taskData.getValue() );
        final int currentIndex = index.incrementAndGet();
        final Set<NodeWithAlloc> matchingNodesForTask = getMatchingNodesForTask( availableByNode, taskData.getTask());
        if ( matchingNodesForTask.isEmpty() ) return null;
        final Tuple<NodeWithAlloc, FileAlignment> result = calculateBestNode(taskData, matchingNodesForTask);
        if ( result == null ) return null;
        final Task task = taskData.getTask();
        availableByNode.get(result.getA()).subFromThis(task.getPod().getRequest());
        taskData.addNs( System.nanoTime()- startTime );
        if ( traceEnabled ){
            task.getTraceRecord().setSchedulerTimeToSchedule((int) (taskData.getTimeInNs() / 1_000_000));
            task.getTraceRecord().setSchedulerPlaceInQueue( currentIndex );
            task.getTraceRecord().setSchedulerLocationCount(
                    taskData.getMatchingFilesAndNodes().getInputsOfTask().getFiles()
                            .parallelStream()
                            .mapToInt( x -> x.locations.size() )
                            .sum()
            );
        }
        return new NodeTaskFilesAlignment(result.getA(), task, result.getB());
    }

    @Override
    public ScheduleObject getTaskNodeAlignment(
            final List<Task> unscheduledTasks,
            final Map<NodeWithAlloc, Requirements> availableByNode
    ){
        AtomicInteger index = new AtomicInteger( 0 );
        final List<NodeTaskAlignment> alignment = unscheduledTasks
                .parallelStream()
                .map( task -> {
                    long startTime = System.nanoTime();
                    final TaskData taskData = calculateTaskData(task, availableByNode);
                    if ( taskData != null ) taskData.addNs( System.nanoTime() - startTime );
                    return taskData;
                } )
                .filter(Objects::nonNull)
                .sorted(Comparator.reverseOrder())
                .sequential()
                .map( taskData -> createNodeAlignment( taskData, availableByNode, index ) )
                .filter(Objects::nonNull)
                .collect(Collectors.toList());
        final ScheduleObject scheduleObject = new ScheduleObject(alignment);
        scheduleObject.setCheckStillPossible( true );
        return scheduleObject;
    }

    Tuple<NodeWithAlloc, FileAlignment> calculateBestNode(
            final TaskData taskData,
            final Set<NodeWithAlloc> matchingNodesForTask
    ){
        FileAlignment bestAlignment = null;
        NodeWithAlloc bestNode = null;
        //Remove all nodes which do not fit anymore
        final List<NodeDataTuple> nodeDataTuples = taskData.getNodeDataTuples();

        for (NodeDataTuple nodeDataTuple : nodeDataTuples) {
            final NodeWithAlloc currentNode = nodeDataTuple.getNode();
            if ( matchingNodesForTask.contains(currentNode) ) {
                final FileAlignment fileAlignment = inputAlignment.getInputAlignment(
                        taskData.getTask(),
                        taskData.getMatchingFilesAndNodes().getInputsOfTask(),
                        currentNode,
                        bestAlignment == null ? Double.MAX_VALUE : bestAlignment.cost
                );

                if ( fileAlignment != null && (bestAlignment == null || bestAlignment.cost < fileAlignment.cost ) ){
                    bestAlignment = fileAlignment;
                    bestNode = currentNode;
                }
            }
        }

        return bestAlignment == null ? null : new Tuple<>( bestNode, bestAlignment );
    }


    /**
     * Create a TaskData object for the input Task
     * @param task
     * @param availableByNode
     * @return null if no nodes available
     */
    TaskData calculateTaskData(
            final Task task,
            final Map<NodeWithAlloc, Requirements> availableByNode
    ) {
        final MatchingFilesAndNodes matchingFilesAndNodes = getMatchingFilesAndNodes(task, availableByNode);
        if ( matchingFilesAndNodes == null || matchingFilesAndNodes.getNodes().isEmpty() ) return null;
        final TaskInputs inputsOfTask = matchingFilesAndNodes.getInputsOfTask();
        long size = inputsOfTask.calculateAvgSize();
        final List<NodeDataTuple> nodeDataTuples = matchingFilesAndNodes
                .getNodes()
                .parallelStream()
                .map(node -> new NodeDataTuple(node, inputsOfTask.calculateDataOnNode( node.getNodeLocation() ) ) )
                .sorted(Comparator.reverseOrder())
                .collect(Collectors.toList());
        final double fracOnNode = nodeDataTuples.get(0).getSizeInBytes() / (double) size;
        final double antiStarvingFactor = 0;
        final double value = fracOnNode + antiStarvingFactor;
        return new TaskData( value, task, nodeDataTuples, matchingFilesAndNodes );
    }

}
