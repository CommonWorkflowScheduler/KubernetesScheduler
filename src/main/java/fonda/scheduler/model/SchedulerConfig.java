package fonda.scheduler.model;

import java.util.List;

public class SchedulerConfig {

    final public String podName;
    final public List<LocalClaim> localClaims;
    final public List<VolumeClaim> volumeClaims;
    final public String workDir;

    private SchedulerConfig(){
        this.podName = null;
        this.localClaims = null;
        this.volumeClaims = null;
        this.workDir = null;
    }

    static public class LocalClaim {
        final public String mountPath;
        final public String hostPath;

        private LocalClaim(){
            this.mountPath = null;
            this.hostPath = null;
        }
    }

    static public class VolumeClaim {
        final public String mountPath;
        final public String claimName;
        final public String subPath;

        private VolumeClaim(){
            this.mountPath = null;
            this.claimName = null;
            this.subPath = null;
        }
    }

}
