package cws.k8s.scheduler.model;

import com.fasterxml.jackson.databind.JsonNode;
import lombok.AccessLevel;
import lombok.NoArgsConstructor;
import lombok.ToString;

import java.util.List;
import java.util.Map;

@ToString
@NoArgsConstructor(access = AccessLevel.PRIVATE,force = true)
public class SchedulerConfig {

    public final List<LocalClaim> localClaims;
    public final List<VolumeClaim> volumeClaims;
    public final String workDir;
    public final String dns;
    public final String copyStrategy;
    public final boolean locationAware;
    public final boolean traceEnabled;
    public final String namespace;
    public final String costFunction;
    public final String strategy;

    public final Integer maxCopyTasksPerNode;

    public final Integer maxWaitingCopyTasksPerNode;
    public final Integer maxHeldCopyTaskReady;
    public final Integer prioPhaseThree;

    public final Map<String, JsonNode> additional;

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
    @NoArgsConstructor(access = AccessLevel.PRIVATE,force = true)
    public static class VolumeClaim {
        public final String mountPath;
        public final String claimName;
        public final String subPath;
    }

}
