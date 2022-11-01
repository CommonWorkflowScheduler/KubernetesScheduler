package fonda.scheduler.scheduler.prioritize;

import fonda.scheduler.model.Task;

import java.util.Collections;
import java.util.List;

public class RandomPrioritize implements Prioritize {

    @Override
    public void sortTasks( List<Task> tasks ) {
        Collections.shuffle( tasks );
    }

}
