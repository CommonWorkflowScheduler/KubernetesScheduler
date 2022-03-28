package fonda.scheduler.model;

import lombok.Getter;
import lombok.ToString;

import java.util.LinkedList;
import java.util.List;
import java.util.Map;

@Getter
@ToString
public class TaskConfig {

    private final String task;
    private final String name;
    private final Map< String, List<Object>> schedulerParams;
    private final TaskInput inputs;
    private final String hash;
    private final float cpus;
    private final long memoryInBytes;

    private TaskConfig() {
        this( null );
    }

    /**
     * Only for testing
     * @param task
     */
    public TaskConfig(String task) {
        this.task = task;
        this.name = null;
        this.schedulerParams = null;
        this.inputs = new TaskInput( null, null, null, new LinkedList<>() );
        this.hash = null;
        this.cpus = 0;
        this.memoryInBytes = 0;
    }

    @Getter
    @ToString
    public static class Input {

        private final String name;
        private final Object value;

        private Input() {
            this.value = null;
            name = null;
        }
    }
}
