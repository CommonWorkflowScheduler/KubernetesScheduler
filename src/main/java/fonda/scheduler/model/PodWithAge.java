package fonda.scheduler.model;

import io.fabric8.kubernetes.api.model.*;
import lombok.Getter;
import lombok.Setter;

import java.math.BigDecimal;
import java.util.List;

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

    public Requirements getRequest(){
        return this
                .getSpec().getContainers().stream()
                .filter( x -> x.getResources() != null
                        && x.getResources().getRequests() != null )
                .map( x ->
                        new Requirements(
                                x.getResources().getRequests().get("cpu") == null ? null : Quantity.getAmountInBytes(x.getResources().getRequests().get("cpu")),
                                x.getResources().getRequests().get("memory") == null ? null : Quantity.getAmountInBytes(x.getResources().getRequests().get("memory"))
                        )
                ).reduce( new Requirements(), Requirements::addToThis );
    }

    public boolean hasFinishedOrFailed(){
        return hasFinishedOrFailed( this );
    }

    public String getName(){
        return this.getMetadata().getName();
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (!(o instanceof PodWithAge)) return false;
        if (!super.equals(o)) return false;

        PodWithAge that = (PodWithAge) o;

        return getName() != null ? getName().equals(that.getName()) : that.getName() == null;
    }

    @Override
    public int hashCode() {
        int result = super.hashCode();
        result = 31 * result + (getAge() != null ? getAge().hashCode() : 0);
        return result;
    }

    public static boolean hasFinishedOrFailed( Pod pod ){
        //init Failure
        final List<ContainerStatus> initContainerStatuses = pod.getStatus().getInitContainerStatuses();
        for ( ContainerStatus containerStatus : initContainerStatuses ) {
            final ContainerStateTerminated terminated = containerStatus.getState().getTerminated();
            if ( terminated != null && terminated.getExitCode() != 0 ){
                return true;
            }
        }
        final List<ContainerStatus> containerStatuses = pod.getStatus().getInitContainerStatuses();
        if ( containerStatuses.isEmpty() ) return false;
        for ( ContainerStatus containerStatus : containerStatuses ) {
            if ( containerStatus.getState().getTerminated() == null ) return false;
        }
        return true;
    }
}
