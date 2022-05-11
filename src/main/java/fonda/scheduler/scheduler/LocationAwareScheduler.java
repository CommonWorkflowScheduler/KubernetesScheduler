package fonda.scheduler.scheduler;

import fonda.scheduler.client.KubernetesClient;
import fonda.scheduler.model.*;
import fonda.scheduler.model.location.Location;
import fonda.scheduler.model.taskinputs.TaskInputs;
import fonda.scheduler.model.tracing.TraceRecord;
import fonda.scheduler.scheduler.data.NodeDataTuple;
import fonda.scheduler.scheduler.data.TaskData;
import fonda.scheduler.scheduler.filealignment.InputAlignment;
import fonda.scheduler.scheduler.filealignment.costfunctions.NoAligmentPossibleException;
import fonda.scheduler.util.*;
import lombok.AccessLevel;
import lombok.Getter;
import lombok.extern.slf4j.Slf4j;

import java.util.*;
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


    /**
     * Find the best alignment for one task
     *
     * @param taskData
     * @param availableByNode
     * @param planedToCopy
     * @param index
     * @return the alignment containing the node and the file alignments
     */
    private NodeTaskFilesAlignment createNodeAlignment (
            final TaskData taskData,
            final Map<NodeWithAlloc, Requirements> availableByNode,
            Map< Location, Map<String, Tuple<Task, Location>>> planedToCopy,
            int index
    ) {
        long startTime = System.nanoTime();
        log.info( "Task: {} has a value of: {}", taskData.getTask().getConfig().getHash(), taskData.getValue() );
        final Set<NodeWithAlloc> matchingNodesForTask = getMatchingNodesForTask( availableByNode, taskData.getTask());
        if ( matchingNodesForTask.isEmpty() ) return null;
        final Tuple<NodeWithAlloc, FileAlignment> result = calculateBestNode(taskData, matchingNodesForTask, planedToCopy);
        if ( result == null ) return null;
        final Task task = taskData.getTask();
        availableByNode.get(result.getA()).subFromThis(task.getPod().getRequest());
        taskData.addNs( System.nanoTime()- startTime );
        if ( traceEnabled ){
            task.getTraceRecord().setSchedulerTimeToSchedule((int) (taskData.getTimeInNs() / 1_000_000));
            task.getTraceRecord().setSchedulerPlaceInQueue( index );
            task.getTraceRecord().setSchedulerLocationCount(
                    taskData.getMatchingFilesAndNodes().getInputsOfTask().getFiles()
                            .parallelStream()
                            .mapToInt( x -> x.locations.size() )
                            .sum()
            );
        }
        return new NodeTaskFilesAlignment(result.getA(), task, result.getB());
    }

    private void addAlignmentToPlanned (
            Map< Location, Map< String, Tuple<Task,Location>> > planedToCopy,
            final Map<Location, AlignmentWrapper> nodeFileAlignment,
            Task task,
            NodeWithAlloc node
    ) {
        for (Map.Entry<Location, AlignmentWrapper> entry : nodeFileAlignment.entrySet()) {
            final Map<String, Tuple<Task, Location>> map = planedToCopy.computeIfAbsent(node.getNodeLocation(), k -> new HashMap<>());
            for (FilePath filePath : entry.getValue().getFilesToCopy()) {
                if ( entry.getKey() != node.getNodeLocation() ) {
                    map.put(filePath.getPath(), new Tuple<>(task, entry.getKey()));
                }
            }
        }
    }

    /**
     * Align all tasks to the best node
     * @param unscheduledTasksSorted
     * @param availableByNode
     * @return a list with alignments
     */
    List<NodeTaskAlignment> createAlignment(
            final List<TaskData> unscheduledTasksSorted,
            final Map<NodeWithAlloc, Requirements> availableByNode
    ){
        int index = 0;
        final List<NodeTaskAlignment> alignment = new LinkedList<>();
        final Map< Location, Map< String, Tuple<Task,Location>> > planedToCopy = new HashMap<>();
        for ( TaskData taskData : unscheduledTasksSorted ){
            final NodeTaskFilesAlignment nodeAlignment = createNodeAlignment(taskData, availableByNode, planedToCopy, index);
            if ( nodeAlignment != null ) {
                alignment.add(nodeAlignment);
                addAlignmentToPlanned( planedToCopy, nodeAlignment.fileAlignment.nodeFileAlignment, taskData.getTask(), nodeAlignment.node );
            }
        }
        return alignment;
    }

    @Override
    public ScheduleObject getTaskNodeAlignment(
            final List<Task> unscheduledTasks,
            final Map<NodeWithAlloc, Requirements> availableByNode
    ){
        final List<TaskData> unscheduledTasksSorted = unscheduledTasks
                .parallelStream()
                .map(task -> {
                    long startTime = System.nanoTime();
                    final TaskData taskData = calculateTaskData(task, availableByNode);
                    if (taskData != null) taskData.addNs(System.nanoTime() - startTime);
                    return taskData;
                })
                .filter(Objects::nonNull)
                .sorted(Comparator.reverseOrder())
                .collect(Collectors.toList());
        final List<NodeTaskAlignment> alignment = createAlignment(unscheduledTasksSorted, availableByNode);
        final ScheduleObject scheduleObject = new ScheduleObject(alignment);
        scheduleObject.setCheckStillPossible( true );
        scheduleObject.setStopSubmitIfOneFails( true );
        return scheduleObject;
    }

    Tuple<NodeWithAlloc, FileAlignment> calculateBestNode(
            final TaskData taskData,
            final Set<NodeWithAlloc> matchingNodesForTask,
            Map< Location, Map<String, Tuple<Task, Location>>> planedToCopy){
        FileAlignment bestAlignment = null;
        NodeWithAlloc bestNode = null;
        //Remove all nodes which do not fit anymore
        final List<NodeDataTuple> nodeDataTuples = taskData.getNodeDataTuples();
        int triedNodes = 0;
        int noAlignmentFound = 0;
        int couldStopFetching = 0;
        final List<Double> costs = traceEnabled ? new LinkedList<>() : null;
        for (NodeDataTuple nodeDataTuple : nodeDataTuples) {
            
            final NodeWithAlloc currentNode = nodeDataTuple.getNode();
            if ( !matchingNodesForTask.contains(currentNode) ) continue;
            final Map<String, Tuple<Task, Location>> currentlyCopying = getCopyingToNode().get(currentNode.getNodeLocation());
            final Map<String, Tuple<Task, Location>> currentlyPlanedToCopy = planedToCopy.get(currentNode.getNodeLocation());
            FileAlignment fileAlignment = null;
            try {
                fileAlignment = inputAlignment.getInputAlignment(
                        taskData.getTask(),
                        taskData.getMatchingFilesAndNodes().getInputsOfTask(),
                        currentNode,
                        currentlyCopying,
                        currentlyPlanedToCopy,
                        bestAlignment == null ? Double.MAX_VALUE : bestAlignment.cost
                );
                if ( fileAlignment == null ){
                    couldStopFetching++;
                } else if ( bestAlignment == null || bestAlignment.cost > fileAlignment.cost ){
                    bestAlignment = fileAlignment;
                    bestNode = currentNode;
                    log.info( "Best alignment for task: {} costs: {}", taskData.getTask().getConfig().getHash(), fileAlignment.cost );
                }
            } catch ( NoAligmentPossibleException e ){
                noAlignmentFound++;
                log.info( "Task: {} - {}", taskData.getTask().getConfig().getName() , e.getMessage() );
            }
            if ( traceEnabled ) {
                triedNodes++;
                final Double thisRoundCost = fileAlignment == null
                        ? null
                        : fileAlignment.cost;
                costs.add( thisRoundCost );
            }
        }

        if ( bestAlignment == null ) return null;
        storeTraceData(
                taskData.getTask().getTraceRecord(),
                triedNodes,
                costs,
                couldStopFetching,
                bestAlignment.cost,
                noAlignmentFound
        );
        return new Tuple<>( bestNode, bestAlignment );
    }

    private void storeTraceData(final TraceRecord traceRecord, int triedNodes, List<Double> costs, int couldStopFetching, double bestCost, int noAlignmentFound){
        if ( !traceEnabled ) return;
        traceRecord.setSchedulerNodesTried( triedNodes );
        traceRecord.setSchedulerNodesCost( costs );
        traceRecord.setSchedulerCouldStopFetching( couldStopFetching );
        traceRecord.setSchedulerBestCost( bestCost );
        traceRecord.setSchedulerNoAlignmentFound( noAlignmentFound );
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
        final double fracOnNode = size == 0 ? 1 : nodeDataTuples.get(0).getSizeInBytes() / (double) size;
        final double antiStarvingFactor = 0;
        final double value = fracOnNode + antiStarvingFactor;
        return new TaskData( value, task, nodeDataTuples, matchingFilesAndNodes );
    }

}
