package cws.k8s.scheduler.model.cluster;

import cws.k8s.scheduler.model.NodeWithAlloc;
import cws.k8s.scheduler.model.Task;
import cws.k8s.scheduler.model.location.NodeLocation;
import lombok.AccessLevel;
import lombok.Getter;
import lombok.NoArgsConstructor;

import java.util.*;

@NoArgsConstructor( access = AccessLevel.PACKAGE )
public class LabelCount {

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
        private final Map<Task,Set<NodeLocation>> taskHasDataOnNode = new HashMap<>();

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
                tasksOnNodeWrapper = new TasksOnNodeWrapper( node.getNodeLocation() );
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
            final HashSet<NodeLocation> locations = new HashSet<>();
            locations.add( task.getNode().getNodeLocation() );
            taskHasDataOnNode.put( task, locations );
        }

    }