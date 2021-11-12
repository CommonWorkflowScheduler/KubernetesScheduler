package fonda.scheduler.model;

import fonda.scheduler.model.location.NodeLocation;
import io.fabric8.kubernetes.api.model.Node;
import io.fabric8.kubernetes.api.model.Pod;
import io.fabric8.kubernetes.api.model.Quantity;
import lombok.Getter;
import lombok.extern.slf4j.Slf4j;

import java.math.BigDecimal;
import java.util.HashMap;
import java.util.Map;

@Getter
@Slf4j
public class NodeWithAlloc extends Node implements Comparable<NodeWithAlloc> {

    private final PodRequirements max_resources;

    Map<String, PodRequirements> assignedPods;

    @Getter
    private final NodeLocation nodeLocation;

    public NodeWithAlloc(Node node) {

        this.setApiVersion( node.getApiVersion() );
        this.setKind( node.getKind() );
        this.setMetadata( node.getMetadata() );
        this.setSpec( node.getSpec() );
        this.setStatus( node.getStatus() );
        for (Map.Entry<String, Object> e : node.getAdditionalProperties().entrySet()) {
            this.setAdditionalProperty( e.getKey(), e.getValue() );
        }

        BigDecimal max_cpu = Quantity.getAmountInBytes(this.getStatus().getAllocatable().get("cpu"));
        BigDecimal max_ram = Quantity.getAmountInBytes(this.getStatus().getAllocatable().get("memory"));

        max_resources = new PodRequirements( max_cpu, max_ram);

        assignedPods = new HashMap<>();

        this.nodeLocation = NodeLocation.getLocation( node );

        log.info("Node {} has RAM: {} and CPU: {}", node.getMetadata().getName(), max_ram, max_cpu);
    }

    public void addPod( Pod pod ){
        PodRequirements request = getRequest(pod);
        synchronized (assignedPods) {
            assignedPods.put( pod.getMetadata().getUid(), request );;
        }
    }

    public void removePod( Pod pod ){
        synchronized (assignedPods) {
            assignedPods.remove( pod.getMetadata().getUid() );
        }
    }

    public PodRequirements getRequestedResources(){
        synchronized (assignedPods) {
            return assignedPods.values().stream()
                    .reduce( new PodRequirements(), PodRequirements::addToThis );
        }
    }

    public PodRequirements getAvailableResources(){
        return max_resources.sub(getRequestedResources());
    }

    private PodRequirements getRequest(final Pod pod){
        return pod
                .getSpec().getContainers().stream()
                .filter( x -> x.getResources() != null
                        && x.getResources().getRequests() != null )
                .map( x ->
                        new PodRequirements(
                                x.getResources().getRequests().get("cpu") == null ? null : Quantity.getAmountInBytes(x.getResources().getRequests().get("cpu")),
                                x.getResources().getRequests().get("memory") == null ? null : Quantity.getAmountInBytes(x.getResources().getRequests().get("memory"))
                        )
                ).reduce( new PodRequirements(), PodRequirements::addToThis );
    }

    public boolean canSchedule( Pod pod ){
        final PodRequirements request = getRequest(pod);
        PodRequirements availableResources = getAvailableResources();
        return request.getRequested_cpu().compareTo(availableResources.getRequested_cpu()) <= 0
                && request.getRequested_ram().compareTo(availableResources.getRequested_ram()) <= 0;
    }

    @Override
    public int compareTo(NodeWithAlloc o) {
        if(getMetadata().getName().equals(o.getMetadata().getName())) {
            return 0;
        } else {
            return -1;
        }
    }

    private static class PodRequirements {

        @Getter
        private BigDecimal requested_cpu;
        @Getter
        private BigDecimal requested_ram;

        public PodRequirements(BigDecimal requested_cpu, BigDecimal requested_ram) {
            this.requested_cpu = requested_cpu == null ? BigDecimal.ZERO : requested_cpu;
            this.requested_ram = requested_ram == null ? BigDecimal.ZERO : requested_ram;
        }

        private PodRequirements(){
            this( BigDecimal.ZERO, BigDecimal.ZERO );
        }

        public PodRequirements addToThis( PodRequirements podRequirements ){
            this.requested_cpu = this.requested_cpu.add(podRequirements.requested_cpu);
            this.requested_ram = this.requested_ram.add(podRequirements.requested_ram);
            return this;
        }

        public PodRequirements addRAMtoThis( BigDecimal requested_ram ){
            this.requested_ram = this.requested_ram.add( requested_ram );
            return this;
        }

        public PodRequirements addCPUtoThis( BigDecimal requested_cpu ){
            this.requested_cpu = this.requested_cpu.add( requested_cpu );
            return this;
        }

        public PodRequirements subFromThis( PodRequirements podRequirements ){
            this.requested_cpu = this.requested_cpu.subtract(podRequirements.requested_cpu);
            this.requested_ram = this.requested_ram.subtract(podRequirements.requested_ram);
            return this;
        }

        public PodRequirements sub( PodRequirements podRequirements ){
            return new PodRequirements(
                this.requested_cpu.subtract(podRequirements.requested_cpu),
                this.requested_ram.subtract(podRequirements.requested_ram)
            );
        }

    }
}
