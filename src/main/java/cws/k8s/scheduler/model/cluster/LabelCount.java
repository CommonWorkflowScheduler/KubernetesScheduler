package cws.k8s.scheduler.model.cluster;

import cws.k8s.scheduler.model.NodeWithAlloc;
import cws.k8s.scheduler.model.Task;
import lombok.AccessLevel;
import lombok.Getter;
import lombok.RequiredArgsConstructor;

import java.util.*;
import java.util.stream.Stream;

/**
 * This class is used to keep track of the number of tasks with a specific label
 * and on which nodes the tasks are running, waiting or there output is stored.
 * This class is not thread safe!
 */
@RequiredArgsConstructor( access = AccessLevel.PACKAGE )
public class LabelCount {

    /**
     * The label that all tasks have
     */
    @Getter
    private final String label;

    /**
     * The tasks that are not yet started
     */
    @Getter
    private final Set<Task> waitingTasks = new HashSet<>();

    /**
     * The tasks that are running
     */
    private final Set<Task> runningTasks = new HashSet<>();

    /**
     * The tasks that are finished
     */
    private final Set<Task> finishedTasks = new HashSet<>();

    /**
     * A Queue with a wrapper that contains the number of tasks running or finished on a node.
     * The queue is sorted by the number of tasks running or finished on a node, such that the node with the most tasks is first.
     */
    @Getter
    private final Queue<TasksOnNodeWrapper> runningOrfinishedOnNodes = new PriorityQueue<>();

    /**
     * For each node store the number of tasks that are running or ran on it,
     * this map is the index for the elements in {@link #runningOrfinishedOnNodes}
     */
    private final Map<NodeWithAlloc, TasksOnNodeWrapper> nodeToShare = new HashMap<>();

    /**
     * For each task with this label store the nodes where the output data of the task is stored
     */
    private final Map<Task,Set<NodeWithAlloc>> taskHasDataOnNode = new HashMap<>();

    /**
     * Get output data for all tasks that are not on the node
     * @param node the node to check
     * @return a stream of DataMissing objects containing the tasks' output data that is missing on the node
     */
    public Stream<DataMissingIntern> tasksNotOnNode( NodeWithAlloc node ) {
        return taskHasDataOnNode.entrySet()
                .stream()
                // check if the task's output is not on the node and the output data was not requested for a real task
                .filter( taskSetEntry -> !taskSetEntry.getValue().contains( node )
                        && !taskSetEntry.getKey().getOutputFiles().isWasRequestedForRealTask() )
                .map( x -> new DataMissingIntern( x.getKey(), node, this ) );
    }

    /**
     * Get the number of tasks with this label that are not yet started
     * @return the number of tasks with this label that are not yet started
     */
    public int getCountWaiting() {
        return waitingTasks.size();
    }

    /**
     * Get the number of tasks with this label that are running
     * @return the number of tasks with this label that are running
     */
    public int getCountRunning() {
        return runningTasks.size();
    }

    /**
     * Get the number of tasks with this label that are finished
     * @return the number of tasks with this label that are finished
     */
    public int getCountFinished() {
        return finishedTasks.size();
    }

    /**
     * Get the number of tasks with this label
     * @return the number of tasks with this label
     */
    public int getCount(){
        return getCountWaiting() + getCountRunning() + getCountFinished();
    }

    /**
     * Add a task to the label count, the task is not yet started
     * the task needs to have this label, however the label is not checked
     * @param task the task to add with this label
     */
    public void addWaitingTask( Task task ){
        waitingTasks.add(task);
    }

    /**
     * Make a task running, the task needs to have this label, however the label is not checked
     * Adds the task to {@link #runningOrfinishedOnNodes}
     * @param task the task to make running
     */
    public void makeTaskRunning( Task task ) {
        final NodeWithAlloc node = task.getNode();
        final TasksOnNodeWrapper tasksOnNodeWrapper = nodeToShare.computeIfAbsent(node, k -> {
            TasksOnNodeWrapper newWrapper = new TasksOnNodeWrapper(k);
            runningOrfinishedOnNodes.add(newWrapper);
            return newWrapper;
        });
        tasksOnNodeWrapper.addRunningTask();
        final boolean remove = waitingTasks.remove( task );
        if ( !remove ) {
            throw new IllegalStateException( "Task " + task + " was not in waiting tasks" );
        }
        runningTasks.add( task );
    }

    /**
     * Make a task finished, the task needs to have this label, however the label is not checked
     * adds the task to {@link #taskHasDataOnNode}, to mark that the output data is on the node
     * @param task the task to make finished
     */
    public void makeTaskFinished( Task task ) {
        final boolean remove = runningTasks.remove( task );
        if ( !remove ) {
            throw new IllegalStateException( "Task " + task + " was not in running tasks" );
        }
        finishedTasks.add( task );
        final HashSet<NodeWithAlloc> locations = new HashSet<>();
        locations.add( task.getNode() );
        taskHasDataOnNode.put( task, locations );
    }

    /**
     * Mark that the task's output data is on a node.
     * This method is called after the task was copied to the node.
     * @param task the task
     * @param node the node the task's output was copied to
     */
    public void taskIsNowOnNode( Task task, NodeWithAlloc node ) {
        taskHasDataOnNode.get( task ).add( node );
    }

}