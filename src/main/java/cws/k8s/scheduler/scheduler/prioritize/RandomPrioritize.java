package cws.k8s.scheduler.scheduler.prioritize;

import cws.k8s.scheduler.model.Task;

import java.util.Collections;
import java.util.List;

public class RandomPrioritize implements Prioritize {

    @Override
    public void sortTasks( List<Task> tasks ) {
        Collections.shuffle( tasks );
    }

}
