package cws.k8s.scheduler.model.cluster;

import cws.k8s.scheduler.model.NodeWithAlloc;
import cws.k8s.scheduler.model.Task;
import lombok.AccessLevel;
import lombok.Getter;
import lombok.RequiredArgsConstructor;

import java.util.*;
import java.util.stream.Stream;

@RequiredArgsConstructor( access = AccessLevel.PACKAGE )
public class LabelCount {

    @Getter
    private final String label;

    @Getter
    private int countFinished = 0;
    @Getter
    private int countStarted = 0;
    @Getter
    private int countWaiting = 0;

    @Getter
    private final Set<Task> waitingTasks = new HashSet<>();
    private final Set<Task> runningTasks = new HashSet<>();
    private final Set<Task> finishedTasks = new HashSet<>();
    private final Set<Task> tasks = new HashSet<>();
    @Getter
    private final Queue<TasksOnNodeWrapper> runningOrfinishedOnNodes = new PriorityQueue<>();
    private final Map<NodeWithAlloc, TasksOnNodeWrapper> nodeToShare = new HashMap<>();
    private final Map<Task,Set<NodeWithAlloc>> taskHasDataOnNode = new HashMap<>();

    public Stream<DataMissing> taskNotOnNode( NodeWithAlloc node ) {
        return taskHasDataOnNode.entrySet()
                .stream()
                .filter( taskSetEntry -> !taskSetEntry.getValue().contains( node )
                        && !taskSetEntry.getKey().getOutputFiles().isWasRequestedForRealTask() )
                .map( x -> new DataMissing( x.getKey(), node, this ) );
    }

    /**
     * Get the number of tasks with this label
     * @return the number of tasks with this label
     */
    public int getCount(){
            return countFinished + countStarted + countWaiting;
        }

    public void addWaitingTask( Task task ){
        countWaiting++;
        tasks.add(task);
        waitingTasks.add(task);
    }

    public void makeTaskRunning( Task task ) {
        final NodeWithAlloc node = task.getNode();
        final TasksOnNodeWrapper tasksOnNodeWrapper;
        if ( nodeToShare.containsKey( node ) ) {
            tasksOnNodeWrapper = nodeToShare.get( node );
        } else {
            tasksOnNodeWrapper = new TasksOnNodeWrapper( node );
            nodeToShare.put( node, tasksOnNodeWrapper );
            runningOrfinishedOnNodes.add( tasksOnNodeWrapper );
        }
        tasksOnNodeWrapper.addRunningTask();
        countWaiting--;
        countStarted++;
        waitingTasks.remove( task );
        runningTasks.add( task );
    }

    public void makeTaskFinished( Task task ) {
        countStarted--;
        countFinished++;
        runningTasks.remove( task );
        finishedTasks.add( task );
        final HashSet<NodeWithAlloc> locations = new HashSet<>();
        locations.add( task.getNode() );
        taskHasDataOnNode.put( task, locations );
    }

    public void taskIsNowOnNode( Task task, NodeWithAlloc node ) {
        taskHasDataOnNode.get( task ).add( node );
    }

}