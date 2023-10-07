package cws.k8s.scheduler.scheduler.la2.copyinadvance;

import cws.k8s.scheduler.model.NodeWithAlloc;
import cws.k8s.scheduler.model.Requirements;
import cws.k8s.scheduler.model.Task;
import cws.k8s.scheduler.model.location.NodeLocation;
import cws.k8s.scheduler.model.location.hierachy.HierarchyWrapper;
import cws.k8s.scheduler.scheduler.filealignment.InputAlignment;
import cws.k8s.scheduler.scheduler.la2.TaskStat;
import cws.k8s.scheduler.util.NodeTaskFilesAlignment;
import cws.k8s.scheduler.util.SortedList;
import cws.k8s.scheduler.util.TaskStats;
import cws.k8s.scheduler.util.copying.CurrentlyCopying;
import cws.k8s.scheduler.util.score.CalculateScore;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.jetbrains.annotations.NotNull;

import java.util.*;
import java.util.stream.Collectors;
import java.util.stream.Stream;

/**
 * This strategy does not perform better than the normal CopyInAdvanceNodeWithMostData.
 */
@Slf4j
public class CopyInAdvanceNodeWithMostDataIntelligent extends CopyInAdvance {

    /**
     * Try to distribute the tasks evenly on the nodes. More important tasks can be on x more nodes. (x = TASKS_READY_FACTOR)
     */
    private static final int TASKS_READY_FACTOR = 2;

    CalculateScore calculateScore;

    HierarchyWrapper hierarchyWrapper;

    public CopyInAdvanceNodeWithMostDataIntelligent(
            CurrentlyCopying currentlyCopying,
            InputAlignment inputAlignment,
            int copySameTaskInParallel ,
            CalculateScore calculateScore,
            HierarchyWrapper hierarchyWrapper
    ) {
        super( currentlyCopying, inputAlignment, copySameTaskInParallel );
        this.calculateScore = calculateScore;
        this.hierarchyWrapper = hierarchyWrapper;
    }

    /**
     * Do not filter maxHeldCopyTaskReady, that could lead to starving if another node has resources.
     */
    public void createAlignmentForTasksWithEnoughCapacity(
            final List<NodeTaskFilesAlignment> nodeTaskFilesAlignments,
            final TaskStats taskStats,
            final CurrentlyCopying planedToCopy,
            final List<NodeWithAlloc> allNodes,
            final int maxCopyingTaskPerNode,
            final int maxHeldCopyTaskReady,
            final Map<NodeLocation, Integer> currentlyCopyingTasksOnNode,
            int prio,
            Map<NodeWithAlloc, List<Task>> readyTasksPerNode
    ) {
        Map<NodeWithAlloc,NodeCache> cache = new HashMap<>();
        long startMethod = System.currentTimeMillis();
        final SortedList<TaskStat> stats = new SortedList<>( taskStats.getTaskStats().stream().filter( x -> x.missingDataOnAnyNode() && !x.isCopyToNodeWithAvailableResources() ).collect( Collectors.toList()) );
        removeTasksThatAreCopiedMoreThanXTimeCurrently( stats, copySameTaskInParallel );

        int readyOnNodes = 0;
        //Outer loop to only process tasks that are not yet ready on enough nodes
        while( !stats.isEmpty() ) {
            LinkedList<TaskStat> tasksReadyOnMoreNodes = new LinkedList<>();
            while( !stats.isEmpty() ) {
                final TaskStat poll = stats.poll();
                if ( poll.dataOnNodes() > readyOnNodes + TASKS_READY_FACTOR ) {
                    tasksReadyOnMoreNodes.add( poll );
                    continue;
                }

                long start = System.currentTimeMillis();
                final TaskStat.NodeAndStatWrapper bestStats = poll.getBestStats();
                final Task task = poll.getTask();
                final NodeWithAlloc node = bestStats.getNode();
                final NodeCache nodeCache = cache.computeIfAbsent( node,
                        n -> new NodeCache(
                                //Only consider workflow tasks, other will not finish soon
                                n.getAssignedPods().entrySet().stream().filter( x -> x.getKey().contains( "||nf-" ) ).map( Map.Entry::getValue ).collect( Collectors.toList() ),
                                readyTasksPerNode.get( n ),
                                planedToCopy.getTasksOnNode( n.getNodeLocation() ),
                                taskStats,
                                node
                        )
                );

                //Check if the node has still enough resources to run the task
                if ( currentlyCopyingTasksOnNode.getOrDefault( node.getNodeLocation(), 0 ) < maxCopyingTaskPerNode
                    && reasonableToCopyData( node, task, nodeCache )
                    && createFileAlignment( planedToCopy, nodeTaskFilesAlignments, currentlyCopyingTasksOnNode, poll, task, node, prio ) )
                {
                        log.info( "Start copy task with {} missing bytes", poll.getBestStats().getTaskNodeStats().getSizeRemaining() );
                        nodeCache.addPlaned( task );
                } else {
                    //if not enough resources or too many tasks are running, mark next node as to compare and add again into the list
                    if ( poll.increaseIndexToCompare() ) {
                        //Only re-add if still other opportunities exist
                        stats.add( poll );
                    }
                }
                task.getTraceRecord().addSchedulerTimeDeltaPhaseThree( (int) (System.currentTimeMillis() - start) );
            }
            stats.addAll( tasksReadyOnMoreNodes );
            readyOnNodes++;
        }
        log.info( "Time to create alignment for tasks with enough capacity: {}", System.currentTimeMillis() - startMethod );
    }

    /**
     * Check if a single task or two tasks could replace one running task
     * @param node
     * @param task
     * @param cache
     * @return
     */
    private boolean reasonableToCopyData( NodeWithAlloc node, Task task, NodeCache cache ) {
        final Requirements availableResources = node.getAvailableResources();
        final ShouldCopyChecker shouldCopyChecker = new ShouldCopyChecker(
                cache.getTaskWithScore( task, "Problem in reasonableToCopyData" ).score,
                cache.waiting,
                task.getRequest()
        );
        return cache.running
                .parallelStream()
                .anyMatch( r -> shouldCopyChecker.couldBeStarted( r.add( availableResources ) ) );
    }

    private class NodeCache {

        /**
         * A <b>sorted</b> list of tasks that are currently running on the node.
         */
        private final Set<Requirements> running;
        /**
         * A <b>sorted</b> list of tasks that are currently waiting on the node.
         */
        private final List<TaskWithScore> waiting;

        private final NodeWithAlloc node;

        private final TaskStats taskStats;

        NodeCache( final Collection<Requirements> running, final List<Task> tasksAlreadyReady, final List<Task> tasksPlanedToCopy, TaskStats taskStats, NodeWithAlloc node ) {
            this.node = node;
            this.taskStats = taskStats;
            /**
             * remove tasks with similar requirements, as replacement would be the same.
             */
            this.running = new HashSet<>(running);

            Stream<TaskWithScore> waitingTemp = null;

            // A list of tasks that are ready to run, or data is copied so that they can start soon.
            if ( tasksAlreadyReady != null ) {
                waitingTemp = tasksAlreadyReady
                        .stream()
                        .map( x -> getTaskWithScore( x, "Problem with tasksAlreadyReady" ) );
            }
            if ( tasksPlanedToCopy != null && !tasksPlanedToCopy.isEmpty() ) {
                final Stream<TaskWithScore> planned = tasksPlanedToCopy.stream()
                        .map( x -> getTaskWithScore( x, "Problem with tasksPlanedToCopy" ) );
                if ( waitingTemp == null ) {
                    waitingTemp = planned;
                } else {
                    waitingTemp = Stream.concat( waitingTemp, planned );
                }
            } else if ( waitingTemp == null ) {
                waitingTemp = Stream.empty();
            }

            waiting = waitingTemp
                    .collect( Collectors.toList() );

        }

        @NotNull
        private TaskWithScore getTaskWithScore( Task task, String error ) {
            if ( taskStats.get( task ) == null ) {
                throw new RuntimeException( error );
            }
            final long dataOnNode = taskStats
                    .get( task )
                    .getInputsOfTask()
                    .calculateDataOnNode( node.getNodeLocation() );
            final long dataInSharedFS = task.getInputSize();
            //The input should be similar to the first scheduling phase
            return new TaskWithScore( task, calculateScore.getScore( task, dataOnNode + dataInSharedFS ) );
        }

        void addPlaned( Task task ) {
            waiting.add( getTaskWithScore( task, "Problem in addPlanned" ) );
        }

    }
    
    @RequiredArgsConstructor
    static class TaskWithScore {
        final Task task;
        final long score;
    }

}

