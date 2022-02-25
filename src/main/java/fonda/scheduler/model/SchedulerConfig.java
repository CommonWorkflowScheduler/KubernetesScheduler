package fonda.scheduler.model;

import java.util.List;

public class SchedulerConfig {

    public final List<LocalClaim> localClaims;
    public final List<VolumeClaim> volumeClaims;
    public final String workDir;
    public final String dns;
    public final String copyStrategy;

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

    public static class LocalClaim {
        public final String mountPath;
        public final String hostPath;

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

    public static class VolumeClaim {
        public final String mountPath;
        public final String claimName;
        public final String subPath;

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
