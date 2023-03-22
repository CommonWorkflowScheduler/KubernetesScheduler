package cws.k8s.scheduler.scheduler.prioritize;

import cws.k8s.scheduler.model.Task;

import java.util.List;

public interface Prioritize {

    void sortTasks( List<Task> tasks);

}
