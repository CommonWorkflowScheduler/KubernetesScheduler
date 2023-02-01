package fonda.scheduler.model;

import lombok.ToString;

import java.util.List;

@ToString
public class SchedulerConfig {

    public final List<VolumeClaim> volumeClaims;
    public final String workDir;
    public final String dns;
    public final boolean traceEnabled;
    public final String namespace;
    public final String costFunction;
    public final String strategy;

    public final Integer maxCopyTasksPerNode;

    public final Integer maxWaitingCopyTasksPerNode;

    private SchedulerConfig() {
        this.volumeClaims = null;
        this.workDir = null;
        this.dns = null;
        this.traceEnabled = false;
        this.costFunction = null;
        this.namespace = null;
        this.strategy = null;
        this.maxCopyTasksPerNode = null;
        this.maxWaitingCopyTasksPerNode = null;
    }

    @ToString
    public static class VolumeClaim {
        public final String mountPath;
        public final String claimName;
        public final String subPath;

        private VolumeClaim(){
            this.mountPath = null;
            this.claimName = null;
            this.subPath = null;
        }

    }

}
