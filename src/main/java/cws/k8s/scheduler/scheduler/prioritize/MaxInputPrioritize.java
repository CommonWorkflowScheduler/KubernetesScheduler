package cws.k8s.scheduler.scheduler.prioritize;

import cws.k8s.scheduler.model.Task;

import java.util.Comparator;
import java.util.List;

public class MaxInputPrioritize implements Prioritize {

    @Override
    public void sortTasks( List<Task> tasks ) {
        tasks.sort( Comparator.comparing( Task::getInputSize ).reversed() );
    }

}
