package fonda.scheduler.model;

import lombok.ToString;

import java.util.List;

@ToString
public class SchedulerConfig {

    public final List<LocalClaim> localClaims;
    public final List<VolumeClaim> volumeClaims;
    public final String workDir;
    public final String dns;
    public final String copyStrategy;
    public final boolean locationAware;
    public final boolean traceEnabled;

    public final String costFunction;

    public SchedulerConfig(List<LocalClaim> localClaims,
                           List<VolumeClaim> volumeClaims,
                           String workDir,
                           String dns,
                           String copyStrategy,
                           boolean locationAware,
                           boolean traceEnabled,
                           String costFunction) {
        this.localClaims = localClaims;
        this.volumeClaims = volumeClaims;
        this.workDir = workDir;
        this.dns = dns;
        this.copyStrategy = copyStrategy;
        this.locationAware = locationAware;
        this.traceEnabled = traceEnabled;
        this.costFunction = costFunction;
    }

    private SchedulerConfig(){
        this(null,null,null,null,null,false,false, null);
    }

    @ToString
    public static class LocalClaim {
        public final String mountPath;
        public final String hostPath;

        private LocalClaim(){
            this.mountPath = null;
            this.hostPath = null;
        }

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
