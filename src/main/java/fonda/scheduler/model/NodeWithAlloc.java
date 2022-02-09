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

    final Map<String, PodRequirements> assignedPods;

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

    public void addPod( PodWithAge pod ){
        PodRequirements request = pod.getRequest();
        synchronized (assignedPods) {
            assignedPods.put( pod.getMetadata().getUid(), request );
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

    public boolean canSchedule( PodWithAge pod ){
        final PodRequirements request = pod.getRequest();
        PodRequirements availableResources = getAvailableResources();
        return request.getCpu().compareTo(availableResources.getCpu()) <= 0
                && request.getRam().compareTo(availableResources.getRam()) <= 0;
    }

    public String getName(){
        return this.getMetadata().getName();
    }

    @Override
    public int compareTo(NodeWithAlloc o) {
        if(getMetadata().getName().equals(o.getMetadata().getName())) {
            return 0;
        } else {
            return -1;
        }
    }

}
