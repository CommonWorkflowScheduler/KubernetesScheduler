package fonda.scheduler.model;

import io.fabric8.kubernetes.api.model.Pod;
import lombok.Getter;
import lombok.Setter;

public class Task {

    @Getter
    private final TaskConfig config;
    @Getter
    private final TaskState state = new TaskState();

    @Getter
    @Setter
    private Pod pod = null;

    @Getter
    @Setter
    private String nodeName = null;

    public Task(TaskConfig config) {
        this.config = config;
    }
}
