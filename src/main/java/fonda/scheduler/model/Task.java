package fonda.scheduler.model;

import fonda.scheduler.model.location.NodeLocation;
import fonda.scheduler.scheduler.util.Batch;
import io.fabric8.kubernetes.api.model.Pod;
import lombok.Getter;
import lombok.Setter;
import org.apache.tomcat.jni.Proc;

public class Task {

    @Getter
    private final TaskConfig config;
    @Getter
    private final TaskState state = new TaskState();

    @Getter
    private final Process process;

    @Getter
    @Setter
    private PodWithAge pod = null;

    @Getter
    @Setter
    private NodeLocation node = null;

    @Getter
    @Setter
    private Batch batch;

    public Task(TaskConfig config) {
        this.config = config;
        this.process = Process.getProcess( config.getTask() );
    }

    public String getWorkingDir(){
        return pod.getSpec().getContainers().get(0).getWorkingDir();
    }

    public boolean wasSuccessfullyExecuted(){
        return pod.getStatus().getContainerStatuses().get( 0 ).getState().getTerminated().getExitCode() == 0;
    }

    @Override
    public String toString() {
        return "Task{" +
                "state=" + state +
                ", pod=" + pod.getMetadata().getName() +
                ", node='" + (node != null ? node.getIdentifier() : "--") + '\'' +
                ", workDir='" + getWorkingDir() + '\'' +
                '}';
    }
}
