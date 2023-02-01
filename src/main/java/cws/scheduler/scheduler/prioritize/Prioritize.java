package fonda.scheduler.scheduler.prioritize;

import fonda.scheduler.model.Task;

import java.util.List;

public interface Prioritize {

    void sortTasks( List<Task> tasks);

}
