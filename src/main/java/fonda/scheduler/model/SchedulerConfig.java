package fonda.scheduler.model;

import java.util.List;

public class SchedulerConfig {

    final public List<LocalClaim> localClaims;
    final public List<VolumeClaim> volumeClaims;
    final public String workDir;
    final public String dns;
    final public String copyStrategy;

    public SchedulerConfig(List<LocalClaim> localClaims,
                           List<VolumeClaim> volumeClaims,
                           String workDir,
                           String dns,
                           String copyStrategy) {
        this.localClaims = localClaims;
        this.volumeClaims = volumeClaims;
        this.workDir = workDir;
        this.dns = dns;
        this.copyStrategy = copyStrategy;
    }

    private SchedulerConfig(){
        this.localClaims = null;
        this.volumeClaims = null;
        this.workDir = null;
        this.dns = null;
        this.copyStrategy = null;
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
