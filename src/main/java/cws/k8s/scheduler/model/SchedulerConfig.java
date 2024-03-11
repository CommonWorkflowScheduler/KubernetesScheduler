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

    public final List<VolumeClaim> volumeClaims;
    public final String workDir;
    public final String dns;
    public final boolean traceEnabled;
    public final String namespace;
    public final String costFunction;
    public final String strategy;
    public final Map<String, JsonNode> additional;

    @ToString
    @NoArgsConstructor(access = AccessLevel.PRIVATE,force = true)
    public static class VolumeClaim {
        public final String mountPath;
        public final String claimName;
        public final String subPath;
    }

}