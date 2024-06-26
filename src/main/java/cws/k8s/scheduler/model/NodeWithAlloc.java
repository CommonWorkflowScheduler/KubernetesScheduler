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
import java.util.*;

@Getter
@Slf4j
public class NodeWithAlloc extends Node implements Comparable<NodeWithAlloc> {

    private final transient CWSKubernetesClient kubernetesClient;

    private static final long serialVersionUID = 1L;

    private final Requirements maxResources;

    private final Map<String, Requirements> assignedPods;

    private final List<PodWithAge> startingTaskCopyingData = new LinkedList<>();

    public NodeWithAlloc( String name ) {
        this.kubernetesClient = null;
        this.maxResources = null;
        this.assignedPods = null;
        this.setMetadata( new ObjectMeta() );
        this.getMetadata().setName( name );
    }

    public NodeWithAlloc( Node node, CWSKubernetesClient kubernetesClient ) {

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

        log.info("Node {} has RAM: {} and CPU: {}", node.getMetadata().getName(), maxRam, maxCpu);
    }

    public void addPod( PodWithAge pod, boolean withStartingTasks ) {
        Requirements request = pod.getRequest();
        if ( withStartingTasks ) {
            synchronized ( startingTaskCopyingData ) {
                if ( !startingTaskCopyingData.contains( pod ) ) {
                    startingTaskCopyingData.add( pod );
                }
            }
        }
        synchronized (assignedPods) {
            assignedPods.put( pod.getMetadata().getUid(), request );
        }
    }

    private void removeStartingTaskCopyingDataByUid( String uid ) {
        synchronized ( startingTaskCopyingData ) {
            final Iterator<PodWithAge> iterator = startingTaskCopyingData.iterator();
            while ( iterator.hasNext() ) {
                final PodWithAge podWithAge = iterator.next();
                if ( podWithAge.getMetadata().getUid().equals( uid ) ) {
                    iterator.remove();
                    break;
                }
            }
        }
    }

    public boolean removePod( Pod pod ){
        removeStartingTaskCopyingDataByUid( pod.getMetadata().getUid() );
        synchronized (assignedPods) {
            return assignedPods.remove( pod.getMetadata().getUid() ) != null;
        }
    }


    public void startingTaskCopyingDataFinished( Task task ) {
        final String uid = task.getPod().getMetadata().getUid();
        removeStartingTaskCopyingDataByUid( uid );
    }

    public int getRunningPods(){
        return assignedPods.size();
    }

    public int getStartingPods(){
        return startingTaskCopyingData.size();
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
