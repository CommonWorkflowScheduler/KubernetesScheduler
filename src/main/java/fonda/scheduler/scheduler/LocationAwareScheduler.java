package fonda.scheduler.scheduler;

import fonda.scheduler.client.KubernetesClient;
import fonda.scheduler.model.*;
import fonda.scheduler.model.location.Location;
import fonda.scheduler.model.location.NodeLocation;
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
            final Map<NodeWithAlloc, Integer> assignedPodsByNode,
            Map< Location, Map<String, Tuple<Task, Location>>> planedToCopy,
            int index
    ) {
        long startTime = System.nanoTime();
        log.info( "Task: {} has a value of: {}", taskData.getTask().getConfig().getRunName(), taskData.getValue() );
        taskData.removeAllNodesWhichHaveNotEnoughResources( availableByNode );
        final Tuple<NodeWithAlloc, FileAlignment> result = calculateBestNode(taskData, planedToCopy, availableByNode, assignedPodsByNode);
        if ( result == null ) {
            return null;
        }
        final Task task = taskData.getTask();
        availableByNode.get(result.getA()).subFromThis(task.getPod().getRequest());
        assignedPodsByNode.put(result.getA(), assignedPodsByNode.getOrDefault(result.getA(),0) + 1);
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
            final PriorityQueue<TaskData> unscheduledTasksSorted,
            final Map<NodeWithAlloc, Requirements> availableByNode
    ){
        int index = 0;
        final List<NodeTaskAlignment> alignment = new LinkedList<>();
        final Map< Location, Map< String, Tuple<Task,Location>> > planedToCopy = new HashMap<>();
        final Map<NodeWithAlloc, Integer> assignedPodsByNode = new HashMap<>();
        while( !unscheduledTasksSorted.isEmpty() ){
            TaskData taskData = unscheduledTasksSorted.poll();
            boolean changed = false;
            log.trace( "TaskData: {}", taskData.getTask().getPod().getName() );
            if ( !taskData.isWeightWasSet() ) {
                log.info( "TaskData: {} weight was not set", taskData.getTask().getPod().getName() );
                final NodeLocation nodeForLabel = outLabelHolder.getNodeForLabel(taskData.getTask().getOutLabel());
                if ( nodeForLabel != null ) {
                    changed = true;
                    taskData.setNodeAndWeight( nodeForLabel, taskData.getTask().getConfig().getOutLabel().getWeight() );
                }
            }
            if ( taskData.calculate( availableByNode ) || changed ){
                if ( !taskData.getNodeDataTuples().isEmpty() ) {
                    unscheduledTasksSorted.add(taskData);
                }
                continue;
            }

            final NodeTaskFilesAlignment nodeAlignment = createNodeAlignment(taskData, availableByNode, assignedPodsByNode, planedToCopy, ++index);
            if ( nodeAlignment != null ) {
                alignment.add(nodeAlignment);
                outLabelHolder.scheduleTaskOnNode( taskData.getTask(), nodeAlignment.node.getNodeLocation() );
                addAlignmentToPlanned( planedToCopy, nodeAlignment.fileAlignment.getNodeFileAlignment(), taskData.getTask(), nodeAlignment.node );
            }
        }
        return alignment;
    }

    @Override
    public ScheduleObject getTaskNodeAlignment(
            final List<Task> unscheduledTasks,
            final Map<NodeWithAlloc, Requirements> availableByNode
    ){
        final PriorityQueue<TaskData> unscheduledTasksSorted = unscheduledTasks
                .parallelStream()
                .map(task -> {
                    long startTime = System.nanoTime();
                    final TaskData taskData = calculateTaskData(task, availableByNode);
                    if (taskData != null) {
                        taskData.addNs(System.nanoTime() - startTime);
                    }
                    return taskData;
                })
                .filter(Objects::nonNull)
                .collect(Collectors.toCollection(() -> new PriorityQueue<>(Collections.reverseOrder())));
        final List<NodeTaskAlignment> alignment = createAlignment(unscheduledTasksSorted, availableByNode);
        final ScheduleObject scheduleObject = new ScheduleObject(alignment);
        scheduleObject.setCheckStillPossible( true );
        scheduleObject.setStopSubmitIfOneFails( true );
        return scheduleObject;
    }

    /**
     * Performs a stalemate between two possible alignments.
     * First check for available cpu, then for the number of running tasks, than for the memory.
     * Decide randomly if all three are equal.
     * @return True if the new alignment is better
     */
    protected boolean stalemate(
            NodeWithAlloc oldNode,
            NodeWithAlloc newNode,
            Map<NodeWithAlloc, Requirements> availableByNode,
            Map<NodeWithAlloc, Integer> assignedPodsByNode,
            Requirements taskRequest )
    {

        //Resources if this node executes the task
        final Requirements availableNodeOld = availableByNode.get( oldNode ).sub( taskRequest );
        final Requirements availableNodeNew = availableByNode.get( newNode ).sub( taskRequest );

        //Calculate percentage of resources that are available
        final double availableCpuOld = availableNodeOld.getCpu().doubleValue() / oldNode.getMaxResources().getCpu().doubleValue();
        final double availableCpuNew = availableNodeNew.getCpu().doubleValue() / newNode.getMaxResources().getCpu().doubleValue();

        double threshold = 0.05;

        if ( availableCpuOld + threshold < availableCpuNew  ) {
            log.trace( "Node {} has less available CPU than node {}", oldNode.getNodeLocation(), newNode.getNodeLocation() );
            return true;
        } else if ( availableCpuOld - threshold > availableCpuNew ) {
            log.trace( "Node {} has more available CPU than node {}", oldNode.getNodeLocation(), newNode.getNodeLocation() );
            return false;
        } else {

            //CPU load comparable, compare number of running tasks

            final int podsOnNewNode = newNode.getRunningPods() + assignedPodsByNode.getOrDefault( newNode, 0 );
            final int podsOnOldNode = oldNode.getRunningPods() + assignedPodsByNode.getOrDefault( oldNode, 0 );

            if ( podsOnNewNode < podsOnOldNode ) {
                log.trace( "Node {} has less running pods than node {}", newNode.getNodeLocation(), oldNode.getNodeLocation() );
                return true;
            } else if ( podsOnNewNode > podsOnOldNode ) {
                log.trace( "Node {} has more running pods than node {}", newNode.getNodeLocation(), oldNode.getNodeLocation() );
                return false;
            } else {

                //CPU and number of running tasks are comparable, compare memory

                final double availableRamOld = availableNodeOld.getRam().doubleValue() / oldNode.getMaxResources().getRam().doubleValue();
                final double availableRamNew = availableNodeNew.getRam().doubleValue() / newNode.getMaxResources().getRam().doubleValue();

                if ( availableRamOld + threshold < availableRamNew ) {
                    log.trace( "Node {} has less available RAM than node {}", oldNode.getNodeLocation(), newNode.getNodeLocation() );
                    return true;
                } else if ( availableRamOld - threshold > availableRamNew ) {
                    log.trace( "Node {} has more available RAM than node {}", oldNode.getNodeLocation(), newNode.getNodeLocation() );
                    return false;
                } else {

                    //Everything is comparable, decide randomly
                    log.trace( "Node {} and node {} are equally comparable -> decide randomly", oldNode.getNodeLocation(), newNode.getNodeLocation() );
                    return Math.random() > 0.5;

                }

            }

        }

    }

    Tuple<NodeWithAlloc, FileAlignment> calculateBestNode(
            final TaskData taskData,
            Map< Location, Map<String, Tuple<Task, Location>>> planedToCopy,
            Map<NodeWithAlloc, Requirements> availableByNode,
            Map<NodeWithAlloc, Integer> assignedPodsByNode
    ) {
        FileAlignment bestAlignment = null;
        NodeWithAlloc bestNode = null;
        boolean bestNodeHasOutLabel = false;
        final List<NodeDataTuple> nodeDataTuples = taskData.getNodeDataTuples();
        int triedNodes = 0;
        int noAlignmentFound = 0;
        int couldStopFetching = 0;
        final List<Double> costs = traceEnabled ? new LinkedList<>() : null;
        for (NodeDataTuple nodeDataTuple : nodeDataTuples) {
            
            final NodeWithAlloc currentNode = nodeDataTuple.getNode();
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
                        bestAlignment == null ? Double.MAX_VALUE : bestAlignment.getCost()
                );

                if ( fileAlignment == null ){
                    couldStopFetching++;
                } else {
                    final boolean isOnOutLabelNode = nodeDataTuple.getNode().getNodeLocation() == taskData.getOutLabelNode();
                    if ( isOnOutLabelNode ) {
                        fileAlignment.setWeight( taskData.getWeight() );
                    }
                    log.info( "Task: {}, outLabelNode: {}, currentNode: {}, bestWeight: {}, currentWeight: {}",
                            taskData.getTask().getConfig().getName(),
                            taskData.getOutLabelNode(),
                            nodeDataTuple.getNode().getNodeLocation(),
                            bestAlignment == null ? null : bestAlignment.getWorth(),
                            fileAlignment.getWorth()
                    );

                    if (    //Not set
                            bestAlignment == null
                            ||
                            // better
                            bestAlignment.getWorth() > fileAlignment.getWorth()
                            ||
                            //Alignment is comparable
                            ( bestAlignment.getWorth() + 1e8 > fileAlignment.getWorth()
                                    &&
                                    //This is the outLabel node
                                    ( isOnOutLabelNode
                                    ||
                                    //Previous node was not the out label node and this alignment wins in stalemate
                                    ( !bestNodeHasOutLabel && stalemate( bestNode, currentNode, availableByNode, assignedPodsByNode, taskData.getTask().getPod().getRequest() ) ) )
                            )
                    ) {
                        bestNodeHasOutLabel = isOnOutLabelNode;
                        bestAlignment = fileAlignment;
                        bestNode = currentNode;
                        log.info( "Best alignment for task: {} costs: {}", taskData.getTask().getConfig().getRunName(), fileAlignment.getCost() );
                    }
                }
            } catch ( NoAligmentPossibleException e ){
                noAlignmentFound++;
                log.info( "Task: {} - {}", taskData.getTask().getConfig().getName() , e.getMessage() );
            }
            if ( traceEnabled ) {
                triedNodes++;
                final Double thisRoundCost = fileAlignment == null
                        ? null
                        : fileAlignment.getCost();
                costs.add( thisRoundCost );
            }
        }

        if ( bestAlignment == null ) {
            return null;
        }
        storeTraceData(
                taskData.getTask().getTraceRecord(),
                triedNodes,
                costs,
                couldStopFetching,
                bestAlignment.getCost(),
                noAlignmentFound
        );
        return new Tuple<>( bestNode, bestAlignment );
    }

    private void storeTraceData(final TraceRecord traceRecord, int triedNodes, List<Double> costs, int couldStopFetching, double bestCost, int noAlignmentFound){
        if ( !traceEnabled ) {
            return;
        }
        traceRecord.setSchedulerNodesTried( triedNodes );
        traceRecord.setSchedulerNodesCost( costs );
        traceRecord.setSchedulerCouldStopFetching( couldStopFetching );
        traceRecord.setSchedulerBestCost( bestCost );
        traceRecord.setSchedulerNoAlignmentFound( noAlignmentFound );
        traceRecord.foundAlignment();
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
        if ( matchingFilesAndNodes == null || matchingFilesAndNodes.getNodes().isEmpty() ) {
            return null;
        }
        final TaskInputs inputsOfTask = matchingFilesAndNodes.getInputsOfTask();
        final long size = inputsOfTask.calculateAvgSize();
        final OutLabel outLabel = task.getConfig().getOutLabel();
        final NodeLocation nodeForLabel = outLabel == null ? null : outLabelHolder.getNodeForLabel(outLabel.getLabel());
        final boolean weightWasSet = outLabel == null || nodeForLabel != null;
        final boolean nodeForLabelNotNull = nodeForLabel != null;
        final double outWeight = outLabel != null ? outLabel.getWeight() : 1.0;
        final List<NodeDataTuple> nodeDataTuples = matchingFilesAndNodes
                .getNodes()
                .parallelStream()
                .map(node -> {
                    double weight = (nodeForLabelNotNull && node.getNodeLocation() != nodeForLabel) ? outWeight : 1.0;
                    return new NodeDataTuple(node, inputsOfTask.calculateDataOnNode(node.getNodeLocation()), weight );
                } )
                .sorted(Comparator.reverseOrder())
                .collect(Collectors.toList());
        final double antiStarvingFactor = 0;
        final TaskData taskData = new TaskData(size, task, nodeDataTuples, matchingFilesAndNodes, antiStarvingFactor, weightWasSet);
        taskData.setOutLabelNode( nodeForLabel );
        taskData.setWeight( outWeight );
        return taskData;
    }

}
