package cws.k8s.scheduler.scheduler;

import cws.k8s.scheduler.model.*;
import cws.k8s.scheduler.scheduler.filealignment.InputAlignment;
import cws.k8s.scheduler.scheduler.filealignment.costfunctions.NoAligmentPossibleException;
import cws.k8s.scheduler.client.KubernetesClient;
import cws.k8s.scheduler.util.*;
import cws.k8s.scheduler.model.location.Location;
import cws.k8s.scheduler.model.location.NodeLocation;
import cws.k8s.scheduler.model.location.hierachy.LocationWrapper;
import cws.k8s.scheduler.model.taskinputs.TaskInputs;
import cws.k8s.scheduler.model.tracing.TraceRecord;
import cws.k8s.scheduler.scheduler.data.NodeDataTuple;
import cws.k8s.scheduler.scheduler.data.TaskData;
import cws.k8s.scheduler.util.copying.CurrentlyCopying;
import cws.k8s.scheduler.util.copying.CurrentlyCopyingOnNode;
import lombok.AccessLevel;
import lombok.Getter;
import lombok.extern.slf4j.Slf4j;

import java.util.*;
import java.util.stream.Collectors;

@Slf4j
public class LocationAwareScheduler extends SchedulerWithDaemonSet {

    @Getter(AccessLevel.PACKAGE)
    private final InputAlignment inputAlignment;
    @Getter(AccessLevel.PACKAGE)
    private final int maxCopyTasksPerNode;
    @Getter(AccessLevel.PACKAGE)
    private final int maxWaitingCopyTasksPerNode;

    public LocationAwareScheduler (
            String name,
            KubernetesClient client,
            String namespace,
            SchedulerConfig config,
            InputAlignment inputAlignment) {
        super( name, client, namespace, config );
        this.inputAlignment = inputAlignment;
        this.maxCopyTasksPerNode = config.maxCopyTasksPerNode == null ? 1 : config.maxCopyTasksPerNode;
        this.maxWaitingCopyTasksPerNode = config.maxWaitingCopyTasksPerNode == null ? 1 : config.maxWaitingCopyTasksPerNode;
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
            CurrentlyCopying planedToCopy,
            int index
    ) {
        long startTime = System.nanoTime();
        log.info( "Task: {} has a value of: {}", taskData.getTask().getConfig().getRunName(), taskData.getValue() );
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
        return new NodeTaskFilesAlignment(result.getA(), task, result.getB(), 100 );
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
        final CurrentlyCopying planedToCopy = new CurrentlyCopying();
        final Map<NodeWithAlloc, Integer> assignedPodsByNode = new HashMap<>();
        Map<NodeWithAlloc,Integer> taskCountWhichCopyToNode = new HashMap<>();
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
            //Calculate the available nodes for this task + their weight. Re-add it to the queue, if it changed
            if ( taskData.calculate( availableByNode ) || changed ){
                if ( !taskData.getNodeDataTuples().isEmpty() ) {
                    unscheduledTasksSorted.add(taskData);
                }
                continue;
            }

            final NodeTaskFilesAlignment nodeAlignment = createNodeAlignment(taskData, availableByNode, assignedPodsByNode, planedToCopy, ++index);
            if ( nodeAlignment != null && onlyAllowXCopyToNodeTasks( nodeAlignment, taskCountWhichCopyToNode, getMaxCopyTasksPerNode() ) ) {
                alignment.add(nodeAlignment);
                outLabelHolder.scheduleTaskOnNode( taskData.getTask(), nodeAlignment.node.getNodeLocation() );
                planedToCopy.addAlignment( nodeAlignment.fileAlignment.getNodeFileAlignment(), taskData.getTask(), nodeAlignment.node );
            }
        }
        return alignment;
    }


    /**
     * Check if the node is already used by x tasks which copy data to it
     * @param nodeTaskAlignments
     * @param taskCountWhichCopyToNode
     * @param maxCopyToNodeTasks
     * @return true if the task can be scheduled on that node and no if already maxCopyToNodeTasks tasks copy to that node
     */
    private boolean onlyAllowXCopyToNodeTasks( NodeTaskFilesAlignment nodeTaskAlignments, Map<NodeWithAlloc,Integer> taskCountWhichCopyToNode, int maxCopyToNodeTasks ){

        final Map<Location, AlignmentWrapper> nodeFileAlignment = nodeTaskAlignments.fileAlignment.getNodeFileAlignment();

        //Does this task needs to copy data to the node?
        boolean copyDataToNode = false;
        for ( Map.Entry<Location, AlignmentWrapper> locationAlignmentWrapperEntry : nodeFileAlignment.entrySet() ) {
            if ( !locationAlignmentWrapperEntry.getValue().getFilesToCopy().isEmpty() ) {
                copyDataToNode = true;
                break;
            }
        }

        //Only keep if there are no more than maxCopyToNodeTasks
        if ( copyDataToNode ) {
            final int alreadyAssignedToNode = taskCountWhichCopyToNode.computeIfAbsent( nodeTaskAlignments.node, NodeWithAlloc::getStartingPods );
            if ( alreadyAssignedToNode >= maxCopyToNodeTasks ) {
                return false;
            } else {
                taskCountWhichCopyToNode.put( nodeTaskAlignments.node, alreadyAssignedToNode + 1 );
            }
        }
        return true;
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
            CurrentlyCopying planedToCopy,
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
            final CurrentlyCopyingOnNode currentlyCopying = getCurrentlyCopying().get(currentNode.getNodeLocation());
            final CurrentlyCopyingOnNode currentlyPlanedToCopy = planedToCopy.get(currentNode.getNodeLocation());
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

    @Override
    boolean assignTaskToNode( NodeTaskAlignment alignment ) {
        final NodeTaskFilesAlignment nodeTaskFilesAlignment = (NodeTaskFilesAlignment) alignment;
        final WriteConfigResult writeConfigResult = writeInitConfig(nodeTaskFilesAlignment);
        if ( writeConfigResult == null ) {
            return false;
        }
        if ( traceEnabled ) {
            traceAlignment( nodeTaskFilesAlignment, writeConfigResult );
        }
        nodeTaskFilesAlignment.setRemoveInit( !writeConfigResult.isWroteConfig() );
        alignment.task.setCopiedFiles( writeConfigResult.getInputFiles() );
        addToCopyingToNode( alignment.task, alignment.node.getNodeLocation(), writeConfigResult.getCopyingToNode() );
        alignment.task.setCopyingToNode( writeConfigResult.getCopyingToNode() );
        if ( writeConfigResult.isWroteConfig() ) {
            getCopyStrategy().generateCopyScript( alignment.task, writeConfigResult.isWroteConfig() );
        }
        alignment.task.setCopiesDataToNode( writeConfigResult.isCopyDataToNode() );
        final List<LocationWrapper> allLocationWrappers = nodeTaskFilesAlignment.fileAlignment.getAllLocationWrappers();
        alignment.task.setInputFiles( allLocationWrappers );
        useLocations( allLocationWrappers );
        return super.assignTaskToNode( alignment );
    }

    private void traceAlignment( NodeTaskFilesAlignment alignment, WriteConfigResult writeConfigResult ) {
        final TraceRecord traceRecord = alignment.task.getTraceRecord();

        int filesOnNodeOtherTask = 0;
        int filesNotOnNode = 0;
        long filesOnNodeOtherTaskByte = 0;
        long filesNotOnNodeByte = 0;

        final NodeLocation currentNode = alignment.node.getNodeLocation();
        for (Map.Entry<Location, AlignmentWrapper> entry : alignment.fileAlignment.getNodeFileAlignment().entrySet()) {
            final AlignmentWrapper alignmentWrapper = entry.getValue();
            if( entry.getKey() == currentNode) {
                traceRecord.setSchedulerFilesNode( alignmentWrapper.getFilesToCopy().size() + alignmentWrapper.getWaitFor().size() );
                traceRecord.setSchedulerFilesNodeBytes( alignmentWrapper.getToCopySize() + alignmentWrapper.getToWaitSize() );
            } else {
                filesOnNodeOtherTask += alignmentWrapper.getWaitFor().size();
                filesOnNodeOtherTaskByte += alignmentWrapper.getToWaitSize();
                filesNotOnNodeByte += alignmentWrapper.getToCopySize();
                filesNotOnNode += alignmentWrapper.getFilesToCopy().size();
            }
        }
        if (traceRecord.getSchedulerFilesNode() == null) {
            traceRecord.setSchedulerFilesNode(0);
        }
        if (traceRecord.getSchedulerFilesNodeBytes() == null) {
            traceRecord.setSchedulerFilesNodeBytes(0l);
        }
        traceRecord.setSchedulerFilesNodeOtherTask(filesOnNodeOtherTask);
        traceRecord.setSchedulerFilesNodeOtherTaskBytes(filesOnNodeOtherTaskByte);
        final int schedulerFilesNode = traceRecord.getSchedulerFilesNode() == null ? 0 : traceRecord.getSchedulerFilesNode();
        traceRecord.setSchedulerFiles(schedulerFilesNode + filesOnNodeOtherTask + filesNotOnNode);
        final long schedulerFilesNodeBytes = traceRecord.getSchedulerFilesNodeBytes() == null ? 0 : traceRecord.getSchedulerFilesNodeBytes();
        traceRecord.setSchedulerFilesBytes(schedulerFilesNodeBytes + filesOnNodeOtherTaskByte + filesNotOnNodeByte);
        traceRecord.setSchedulerDependingTask( (int) writeConfigResult.getWaitForTask().values().stream().distinct().count() );
        traceRecord.setSchedulerNodesToCopyFrom( alignment.fileAlignment.getNodeFileAlignment().size() - (schedulerFilesNode > 0 ? 1 : 0) );
    }

    @Override
    public boolean canSchedulePodOnNode( Requirements availableByNode, PodWithAge pod, NodeWithAlloc node ) {
        return this.getDaemonIpOnNode( node ) != null && super.canSchedulePodOnNode( availableByNode, pod, node );
    }


}
