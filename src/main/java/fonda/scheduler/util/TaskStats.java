package fonda.scheduler.util;

import fonda.scheduler.model.Task;
import fonda.scheduler.scheduler.la2.TaskStat;

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

}
