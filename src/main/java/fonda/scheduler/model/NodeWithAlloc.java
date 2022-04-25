package fonda.scheduler.model;

import fonda.scheduler.client.KubernetesClient;
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

    private final KubernetesClient kubernetesClient;

    private static final long serialVersionUID = 1L;

    private final Requirements maxResources;

    private final Map<String, Requirements> assignedPods;

    @Getter
    private final NodeLocation nodeLocation;

    public NodeWithAlloc( Node node, KubernetesClient kubernetesClient ) {

        this.kubernetesClient = kubernetesClient;

        this.setApiVersion( node.getApiVersion() );
        this.setKind( node.getKind() );
        this.setMetadata( node.getMetadata() );
        this.setSpec( node.getSpec() );
        this.setStatus( node.getStatus() );
        for (Map.Entry<String, Object> e : node.getAdditionalProperties().entrySet()) {
            this.setAdditionalProperty( e.getKey(), e.getValue() );
        }

        BigDecimal maxCpu = Quantity.getAmountInBytes(this.getStatus().getAllocatable().get("cpu"));
        BigDecimal maxRam = Quantity.getAmountInBytes(this.getStatus().getAllocatable().get("memory"));

        maxResources = new Requirements( maxCpu, maxRam);

        assignedPods = new HashMap<>();

        this.nodeLocation = NodeLocation.getLocation( node );

        log.info("Node {} has RAM: {} and CPU: {}", node.getMetadata().getName(), maxRam, maxCpu);
    }

    public void addPod( PodWithAge pod ){
        Requirements request = pod.getRequest();
        synchronized (assignedPods) {
            assignedPods.put( pod.getMetadata().getUid(), request );
        }
    }

    public void removePod( Pod pod ){
        synchronized (assignedPods) {
            assignedPods.remove( pod.getMetadata().getUid() );
        }
    }

    /**
     * @return max(Requested by all and currently used )
     */
    public Requirements getRequestedResources(){
        final Requirements requestedByPods;
        synchronized (assignedPods) {
            requestedByPods = assignedPods.values().stream()
                    .reduce(new Requirements(), Requirements::addToThis);
        }
        final BigDecimal currentMemoryOfNode = kubernetesClient.getMemoryOfNode( this );
        if ( requestedByPods.getRam().compareTo(currentMemoryOfNode) == -1 ){
            requestedByPods.addRAMtoThis(currentMemoryOfNode.subtract(requestedByPods.getRam()));
        }
        return requestedByPods;
    }

    public Requirements getAvailableResources(){
        return maxResources.sub(getRequestedResources());
    }

    public boolean canSchedule( PodWithAge pod ){
        final Requirements request = pod.getRequest();
        Requirements availableResources = getAvailableResources();
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

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (!(o instanceof NodeWithAlloc)) return false;
        if (!super.equals(o)) return false;
        NodeWithAlloc that = (NodeWithAlloc) o;
        return getMetadata().getName() != null ? getMetadata().getName().equals(that.getMetadata().getName()) : that.getMetadata().getName() == null;
    }

    @Override
    public int hashCode() {
        int result = super.hashCode();
        result = 31 * result + (getMaxResources() != null ? getMaxResources().hashCode() : 0);
        result = 31 * result + (getAssignedPods() != null ? getAssignedPods().hashCode() : 0);
        result = 31 * result + (getNodeLocation() != null ? getNodeLocation().hashCode() : 0);
        return result;
    }
}
