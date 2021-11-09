package fonda.scheduler.model;

import java.util.List;

public class SchedulerConfig {

    final public List<LocalClaim> localClaims;
    final public List<VolumeClaim> volumeClaims;
    final public String workDir;

    public SchedulerConfig(List<LocalClaim> localClaims, List<VolumeClaim> volumeClaims, String workDir) {
        this.localClaims = localClaims;
        this.volumeClaims = volumeClaims;
        this.workDir = workDir;
    }

    private SchedulerConfig(){
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

        @Override
        public String toString() {
            return "LocalClaim{" +
                    "mountPath='" + mountPath + '\'' +
                    ", hostPath='" + hostPath + '\'' +
                    '}';
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

        @Override
        public String toString() {
            return "VolumeClaim{" +
                    "mountPath='" + mountPath + '\'' +
                    ", claimName='" + claimName + '\'' +
                    ", subPath='" + subPath + '\'' +
                    '}';
        }
    }

    @Override
    public String toString() {
        return "SchedulerConfig{" +
                "localClaims=" + localClaims +
                ", volumeClaims=" + volumeClaims +
                ", workDir='" + workDir + '\'' +
                '}';
    }
}
