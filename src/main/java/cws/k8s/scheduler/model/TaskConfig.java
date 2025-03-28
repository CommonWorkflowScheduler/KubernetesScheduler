package cws.k8s.scheduler.model;

import lombok.AccessLevel;
import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.ToString;

import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Set;

@Getter
@ToString
public class TaskConfig {

    private final String task;
    private final String name;
    private final Map< String, List<Object>> schedulerParams;
    private final TaskInput inputs;
    private final String runName;
    private final float cpus;
    private final long memoryInBytes;
    private final Set<String> outLabel;
    private final String workDir;
    private final int repetition;
    private final Long inputSize;

    private TaskConfig() {
        this( null );
    }

    /**
     * Only for testing
     */
    public TaskConfig(String task) {
        this.task = task;
        this.name = null;
        this.schedulerParams = null;
        this.inputs = new TaskInput( null, null, null, new LinkedList<>() );
        this.runName = null;
        this.cpus = 0;
        this.memoryInBytes = 0;
        this.workDir = null;
        this.outLabel = null;
        this.repetition = 0;
        this.inputSize = null;
    }

    public TaskConfig(String task, String name, String workDir, String runName ) {
        this.task = task;
        this.name = name;
        this.schedulerParams = null;
        this.inputs = new TaskInput( null, null, null, new LinkedList<>() );
        this.runName = runName;
        this.cpus = 0;
        this.memoryInBytes = 0;
        this.workDir = workDir;
        this.outLabel = null;
        this.repetition = 0;
        this.inputSize = null;
    }

    @Getter
    @ToString
    @NoArgsConstructor( access = AccessLevel.PRIVATE, force = true )
    public static class Input {

        private final String name;
        private final Object value;

    }
}
