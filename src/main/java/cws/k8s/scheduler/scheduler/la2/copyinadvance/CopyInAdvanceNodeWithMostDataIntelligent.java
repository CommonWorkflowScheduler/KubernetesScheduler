package cws.k8s.scheduler.scheduler.la2.copyinadvance;

import cws.k8s.scheduler.model.NodeWithAlloc;
import cws.k8s.scheduler.model.Requirements;
import cws.k8s.scheduler.model.Task;
import cws.k8s.scheduler.model.location.NodeLocation;
import cws.k8s.scheduler.scheduler.filealignment.InputAlignment;
import cws.k8s.scheduler.scheduler.la2.TaskStat;
import cws.k8s.scheduler.util.NodeTaskFilesAlignment;
import cws.k8s.scheduler.util.SortedList;
import cws.k8s.scheduler.util.TaskStats;
import cws.k8s.scheduler.util.copying.CurrentlyCopying;
import lombok.extern.slf4j.Slf4j;

import java.util.*;
import java.util.stream.Collectors;

@Slf4j
public class CopyInAdvanceNodeWithMostDataIntelligent extends CopyInAdvance {

    /**
     * Try to distribute the tasks evenly on the nodes. More important tasks can be on x more nodes. (x = TASKS_READY_FACTOR)
     */
    private static final int TASKS_READY_FACTOR = 2;
    private static final RequirementsComparator requirementsComparator = new RequirementsComparator();

    public CopyInAdvanceNodeWithMostDataIntelligent(
            CurrentlyCopying currentlyCopying,
            InputAlignment inputAlignment,
            int copySameTaskInParallel ) {
        super( currentlyCopying, inputAlignment, copySameTaskInParallel );
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
                long start = System.currentTimeMillis();

                if ( poll.dataOnNodes() > readyOnNodes + TASKS_READY_FACTOR ) {
                    tasksReadyOnMoreNodes.add( poll );
                    continue;
                }

                final TaskStat.NodeAndStatWrapper bestStats = poll.getBestStats();
                final Task task = poll.getTask();
                final NodeWithAlloc node = bestStats.getNode();
                final NodeCache nodeCache = cache.computeIfAbsent( node,
                        n -> new NodeCache(
                                //Only consider workflow tasks, other will not finish soon
                                n.getAssignedPods().entrySet().stream().filter( x -> x.getKey().contains( "||nf-" ) ).map( x -> x.getValue() ).collect( Collectors.toList() ),
                                readyTasksPerNode.get( n ),
                                planedToCopy.getTasksOnNode( n.getNodeLocation() )
                        )
                );

                //Check if the node has still enough resources to run the task
                if ( currentlyCopyingTasksOnNode.getOrDefault( node.getNodeLocation(), 0 ) < maxCopyingTaskPerNode
                    && reasonableToCopyData( node, task, nodeCache )
                    && createFileAlignment( planedToCopy, nodeTaskFilesAlignments, currentlyCopyingTasksOnNode, poll, task, node, prio ) )
                {
                        log.info( "Start copy task with {} missing bytes", poll.getBestStats().getTaskNodeStats().getSizeRemaining() );
                        nodeCache.addPlaned( task.getRequest() );
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

    private boolean reasonableToCopyData( NodeWithAlloc node, Task task, NodeCache cache ) {
        return true;
    }

    private static class NodeCache {

        /**
         * A <b>sorted</b> list of tasks that are currently running on the node.
         */
        private final List<Requirements> running;
        /**
         * A <b>sorted</b> list of tasks that are currently waiting on the node.
         */
        private final ArrayList<Requirements> waiting;

        NodeCache( final Collection<Requirements> running, final List<Task> tasksAlreadyReady, final List<Task> tasksPlanedToCopy ) {

            this.running = new LinkedList<>(running);
            this.running.sort( requirementsComparator );

            ArrayList<Requirements> waitingTemp = null;

            // A list of tasks that are ready to run, or data is copied so that they can start soon.
            if ( tasksAlreadyReady != null ) {
                waitingTemp = tasksAlreadyReady.stream().map( Task::getRequest ).collect( Collectors.toCollection(ArrayList::new) );
            }
            if ( tasksPlanedToCopy != null && !tasksPlanedToCopy.isEmpty() ) {
                final ArrayList<Requirements> planned = tasksPlanedToCopy.stream().map( Task::getRequest ).collect( Collectors.toCollection(ArrayList::new) );
                if ( waitingTemp == null ) {
                    waitingTemp = planned;
                } else {
                    waitingTemp.addAll( planned );
                }
            } else if ( waitingTemp == null ) {
                waitingTemp = new ArrayList<>();
            }

            waiting = waitingTemp;
            waiting.sort( requirementsComparator );

        }

        void addPlaned( Requirements requirement ) {
            int insertionIndex = Collections.binarySearch( waiting, requirement, requirementsComparator );
            if (insertionIndex < 0) {
                insertionIndex = -(insertionIndex + 1);
            }
            waiting.add(insertionIndex, requirement);
        }

    }

    private static class RequirementsComparator implements Comparator<Requirements> {

        @Override
        public int compare(Requirements r1, Requirements r2) {
            int cpuComparison = r1.getCpu().compareTo(r2.getCpu());
            if (cpuComparison != 0) {
                return cpuComparison;
            } else {
                return r1.getRam().compareTo(r2.getRam());
            }
        }

    }


}

