package fonda.scheduler.model;

import io.fabric8.kubernetes.api.model.ObjectMeta;
import io.fabric8.kubernetes.api.model.Pod;
import io.fabric8.kubernetes.api.model.PodSpec;
import io.fabric8.kubernetes.api.model.PodStatus;
import lombok.Getter;
import lombok.Setter;

import java.math.BigDecimal;

public class PodWithAge extends Pod {

    @Getter
    @Setter
    private BigDecimal age;

    public PodWithAge(ObjectMeta metadata, PodSpec spec, PodStatus status) {
        super("v1", "Pod",  metadata, spec, status);
        this.age = BigDecimal.ZERO;
    }
    public PodWithAge() {
        super("v1", "Pod",  null, null, null);
        this.age = BigDecimal.ZERO;
    }
    public PodWithAge(Pod pod) {
        super(pod.getApiVersion(), pod.getKind(), pod.getMetadata(), pod.getSpec(), pod.getStatus());
        this.age = BigDecimal.ZERO;
    }
}
