package cws.k8s.scheduler.util;

import cws.k8s.scheduler.scheduler.la2.TaskStat;
import cws.k8s.scheduler.scheduler.la2.TaskStatComparator;
import cws.k8s.scheduler.model.Task;

import java.util.*;

public class TaskStats {

    private final Map<Task, TaskStat> taskStats = new HashMap<>();

    public TaskStat get( Task task ) {
        return this.taskStats.get( task );
    }


    public void add( TaskStat taskStat ) {
        this.taskStats.put( taskStat.getTask(), taskStat );
    }

    public Collection<TaskStat> getTaskStats() {
        return this.taskStats.values();
    }

    /**
     * This method will remove all tasks, that have set copyToNodeWithAvailableResources
     */
    public void removeTasksThatHaveBeenStarted() {
        taskStats.entrySet().removeIf(entry -> entry.getValue().isCopyToNodeWithAvailableResources());
    }

    public void setComparator( TaskStatComparator comparator ) {
        for ( TaskStat taskStat : taskStats.values() ) {
            taskStat.setComparator( comparator );
        }
    }

}
