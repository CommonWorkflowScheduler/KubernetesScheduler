package cws.k8s.scheduler.model;

import cws.k8s.scheduler.client.CWSKubernetesClient;
import io.fabric8.kubernetes.api.model.Node;
import io.fabric8.kubernetes.api.model.ObjectMeta;
import io.fabric8.kubernetes.api.model.Pod;
import io.fabric8.kubernetes.api.model.Quantity;
import io.fabric8.kubernetes.client.readiness.Readiness;
import lombok.Getter;
import lombok.extern.slf4j.Slf4j;

import java.math.BigDecimal;
import java.util.HashMap;
import java.util.Map;

@Getter
@Slf4j
public class NodeWithAlloc extends Node implements Comparable<NodeWithAlloc> {

    private final transient CWSKubernetesClient kubernetesClient;

    private static final long serialVersionUID = 1L;

    private Requirements maxResources;

    private final Map<String, Requirements> assignedPods;

    public NodeWithAlloc( String name ) {
        this.kubernetesClient = null;
        this.maxResources = null;
        this.assignedPods = null;
        this.setMetadata( new ObjectMeta() );
        this.getMetadata().setName( name );
    }

    public NodeWithAlloc( Node node, CWSKubernetesClient kubernetesClient ) {

        this.kubernetesClient = kubernetesClient;

        setNodeData( node, true );

        assignedPods = new HashMap<>();

    }


    public void update( Node node ) {
        setNodeData( node, false );
    }

    /**
     * Update the node with the new data
     * @param node the new node data
     * @param isCreate if the node is an update, false if it is a new node
     */
    private void setNodeData( Node node, boolean isCreate ) {
        if ( node == null ) {
            return;
        }
        if ( isCreate || !node.getApiVersion().equals( this.getApiVersion() ) ) {
            if ( !isCreate) log.info( "Updating apiVersion for node {} from {} to {}", this.getName(), this.getApiVersion(), node.getApiVersion());
            this.setApiVersion( node.getApiVersion() );
        }
        if ( isCreate || !node.getKind().equals( this.getKind() ) ) {
            if ( !isCreate) log.info( "Updating kind for node {} from {} to {}", this.getName(), this.getKind(), node.getKind());
            this.setKind( node.getKind() );
        }
        if ( isCreate || !node.getMetadata().equals( this.getMetadata() ) ) {
            if ( !isCreate) log.debug( "Updating metadata for node {} from {} to {}", this.getName(), this.getMetadata(), node.getMetadata());
            this.setMetadata( node.getMetadata() );
        }
        if ( isCreate || !node.getSpec().equals( this.getSpec() ) ) {
            if ( !isCreate) log.info( "Updating spec for node {} from {} to {}", this.getName(), this.getSpec(), node.getSpec());
            this.setSpec( node.getSpec() );
        }
        if ( isCreate || !node.getStatus().equals( this.getStatus() ) ) {
            if ( !isCreate) log.debug( "Updating status for node {} from {} to {}", this.getName(), this.getStatus(), node.getStatus());
            this.setStatus( node.getStatus() );
        }
        for (Map.Entry<String, Object> e : node.getAdditionalProperties().entrySet()) {
            if ( this.getAdditionalProperties().containsKey( e.getKey() )) {
                continue;
            }
            if ( !isCreate) log.info( "Updating additional property {} for node {} from {} to {}", e.getKey(), this.getName(), this.getAdditionalProperties().get( e.getKey() ), e.getValue());
            this.setAdditionalProperty( e.getKey(), e.getValue() );
        }

        if ( isCreate
                || !Quantity.getAmountInBytes(node.getStatus().getAllocatable().get("cpu")).equals(  maxResources.getCpu() )
                || !Quantity.getAmountInBytes(node.getStatus().getAllocatable().get("memory")).equals(  maxResources.getRam() ) ) {
            if ( !isCreate) log.info( "Updating max resources for node {} from {} to {}", this.getName(), maxResources, node.getStatus().getAllocatable());
            BigDecimal maxCpu = Quantity.getAmountInBytes( this.getStatus().getAllocatable().get( "cpu" ) );
            BigDecimal maxRam = Quantity.getAmountInBytes( this.getStatus().getAllocatable().get( "memory" ) );
            maxResources = new Requirements( maxCpu, maxRam );
        }
    }

    public void addPod( PodWithAge pod ) {
        Requirements request = pod.getRequest();
        synchronized (assignedPods) {
            assignedPods.put( pod.getMetadata().getUid(), request );
        }
    }

    public boolean removePod( Pod pod ){
        synchronized (assignedPods) {
            return assignedPods.remove( pod.getMetadata().getUid() ) != null;
        }
    }

    public int getRunningPods(){
        return assignedPods.size();
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
        return requestedByPods;
    }

    public Requirements getAvailableResources(){
        return maxResources.sub(getRequestedResources());
    }

    public boolean canSchedule( final Requirements request ){
        Requirements availableResources = getAvailableResources();
        return request.getCpu().compareTo(availableResources.getCpu()) <= 0
                && request.getRam().compareTo(availableResources.getRam()) <= 0;
    }

    public String getName(){
        return this.getMetadata().getName();
    }

    @Override
    public int compareTo(NodeWithAlloc o) {
        return getMetadata().getName().compareTo( o.getMetadata().getName() );
    }

    public boolean canScheduleNewPod(){
        return isReady() && ( getSpec().getUnschedulable() == null || !getSpec().getUnschedulable() );
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (!(o instanceof NodeWithAlloc)) {
            return false;
        }
        if (!super.equals(o)) {
            return false;
        }
        NodeWithAlloc that = (NodeWithAlloc) o;
        return getMetadata().getName() != null ? getMetadata().getName().equals(that.getMetadata().getName()) : that.getMetadata().getName() == null;
    }

    @Override
    public int hashCode() {
        int result = super.hashCode();
        result = 31 * result + (getMaxResources() != null ? getMaxResources().hashCode() : 0);
        result = 31 * result + (getAssignedPods() != null ? getAssignedPods().hashCode() : 0);
        return result;
    }

    public boolean isReady(){
        return Readiness.isNodeReady(this);
    }
}
