package cws.k8s.scheduler.client;

import cws.k8s.scheduler.model.NodeWithAlloc;
import cws.k8s.scheduler.model.PodWithAge;
import cws.k8s.scheduler.model.Task;
import io.fabric8.kubernetes.api.model.*;
import io.fabric8.kubernetes.client.DefaultKubernetesClient;
import io.fabric8.kubernetes.client.KubernetesClientException;
import io.fabric8.kubernetes.client.Watcher;
import io.fabric8.kubernetes.client.WatcherException;
import lombok.extern.slf4j.Slf4j;

import java.math.BigDecimal;
import java.util.*;

@Slf4j
public class KubernetesClient extends DefaultKubernetesClient  {

    private final Map<String, NodeWithAlloc> nodeHolder= new HashMap<>();
    private final List<Informable> informables = new LinkedList<>();

    public KubernetesClient(){
        for( Node node : this.nodes().list().getItems() ){
            nodeHolder.put( node.getMetadata().getName(), new NodeWithAlloc(node,this) );
        }
        this.pods().inAnyNamespace().watch( new PodWatcher( this ) );
        this.nodes().watch( new NodeWatcher( this ) );
    }

    public void addInformable( Informable informable ){
        synchronized ( informables ){
            informables.add( informable );
        }
    }

    public void removeInformable( Informable informable ){
        synchronized ( informables ){
            informables.remove( informable );
        }
    }

    private void informAllInformable(){
        synchronized ( informables ){
            for (Informable informable : informables ) {
                informable.informResourceChange();
            }
        }
    }

    private void informAllNewNode( NodeWithAlloc node ){
        synchronized ( informables ){
            for (Informable informable : informables ) {
                informable.newNode( node );
            }
        }
    }

    private void informAllRemovedNode( NodeWithAlloc node ){
        synchronized ( informables ){
            for (Informable informable : informables ) {
                informable.removedNode( node );
            }
        }
    }

    public int getNumberOfNodes(){
        return this.nodeHolder.size();
    }

    public void assignPodToNode( PodWithAge pod, String node ) {
        for ( int i = 0; i < 5; i++ ) {
            try {
                Thread.sleep( 1000L * (int) (Math.pow( 2, i ) - 1) );
            } catch ( InterruptedException e ) {
                Thread.currentThread().interrupt();
            }
            try {
                final NodeWithAlloc nodeWithAlloc = nodeHolder.get( node );
                final Binding build = new BindingBuilder()
                        .withNewMetadata().withName( pod.getName() ).endMetadata()
                        .withNewTarget()
                        .withKind( nodeWithAlloc.getKind() )
                        .withApiVersion( nodeWithAlloc.getApiVersion() )
                        .withName( node ).endTarget()
                        .build();
                bindings()
                        .inNamespace( pod.getMetadata().getNamespace() )
                        .resource( build )
                        .create();
                return;
            } catch ( KubernetesClientException e ) {
                if ( e.getStatus().getMessage().toLowerCase().contains( "is already assigned to node" ) ) {
                    // If node is already assigned, ignore (happens if binding timeouts)
                    return;
                }
                e.printStackTrace();
                if ( i == 4 ) {
                    throw e;
                }
            }
        }
    }

    public List<NodeWithAlloc> getAllNodes(){
        return new ArrayList<>(this.nodeHolder.values());
    }

    public BigDecimal getMemoryOfNode(NodeWithAlloc node ){
        final Quantity memory = this
                .top()
                .nodes()
                .metrics(node.getName())
                .getUsage()
                .get("memory");
        return Quantity.getAmountInBytes(memory);
    }

    static class NodeWatcher implements Watcher<Node>{

        private final KubernetesClient kubernetesClient;

        public NodeWatcher(KubernetesClient kubernetesClient) {
            this.kubernetesClient = kubernetesClient;
        }

        @Override
        public void eventReceived(Action action, Node node) {
            boolean change = false;
            NodeWithAlloc processedNode = null;
            switch (action) {
                case ADDED:
                    log.info("New Node {} was added", node.getMetadata().getName());
                    synchronized ( kubernetesClient.nodeHolder ){
                        if ( ! kubernetesClient.nodeHolder.containsKey( node.getMetadata().getName() ) ){
                            processedNode = new NodeWithAlloc(node,kubernetesClient);
                            kubernetesClient.nodeHolder.put( node.getMetadata().getName(), processedNode );
                            change = true;
                        }
                    }
                    if ( change ) {
                        kubernetesClient.informAllNewNode( processedNode );
                    }
                    break;
                case DELETED:
                    log.info("Node {} was deleted", node.getMetadata().getName());
                    synchronized ( kubernetesClient.nodeHolder ){
                        if ( kubernetesClient.nodeHolder.containsKey( node.getMetadata().getName() ) ){
                            processedNode  = kubernetesClient.nodeHolder.remove( node.getMetadata().getName() );
                            change = true;
                        }
                    }
                    if ( change ) {
                        kubernetesClient.informAllRemovedNode( processedNode );
                    }
                    break;
                case ERROR:
                    log.info("Node {} has an error", node.getMetadata().getName());
                    //todo deal with error
                    break;
                case MODIFIED:
                    log.info("Node {} was modified", node.getMetadata().getName());
                    //todo deal with changed state
                    break;
                default: log.warn("No implementation for {}", action);
            }
        }

        @Override
        public void onClose(WatcherException cause) {
            log.info( "Watcher was closed" );
        }
    }

    static class PodWatcher implements Watcher<Pod> {

        private final KubernetesClient kubernetesClient;

        public PodWatcher(KubernetesClient kubernetesClient) {
            this.kubernetesClient = kubernetesClient;
        }

        @Override
        public void eventReceived(Action action, Pod pod) {
            String nodeName = pod.getSpec().getNodeName();
            if( nodeName != null ){
                NodeWithAlloc node = kubernetesClient.nodeHolder.get( pod.getSpec().getNodeName() );
                switch ( action ){
                    case ADDED:
                        if ( !PodWithAge.hasFinishedOrFailed( pod ) ) {
                            node.addPod(new PodWithAge(pod));
                        }
                        break;
                    case MODIFIED:
                        final List<ContainerStatus> containerStatuses = pod.getStatus().getContainerStatuses();
                        if ( !PodWithAge.hasFinishedOrFailed( pod ) ) {
                            break;
                        }
                        //Pod is finished
                    case DELETED:
                    case ERROR:
                        //Delete Pod in any case
                        if ( node.removePod( pod ) ){
                            log.info("Pod has released its resources: {}", pod.getMetadata().getName());
                            kubernetesClient.informAllInformable();
                        }
                        break;
                    default: log.warn("No implementation for {}", action);
                }

            }
        }


        @Override
        public void onClose(WatcherException cause) {
            log.info( "Watcher was closed" );
        }

    }

    /**
     * After some testing, this was found to be the only reliable way to patch a pod
     * using the Kubernetes client.
     * 
     * It will create a patch for the memory limits and request values and submit it
     * to the cluster.
     * 
     * @param t          the task to be patched
     * @return false if patching failed because of InPlacePodVerticalScaling
     */
    public boolean patchTaskMemory( Task t ) {
        final PodWithAge pod = t.getPod();
        String namespace = pod.getMetadata().getNamespace();
        String podname = pod.getName();
        final String valueAsString = t.getPlanedRequirements().getRam().toPlainString();
        log.debug("namespace: {}, podname: {}", namespace, podname);
        // @formatter:off
        String patch = "kind: Pod\n"
                + "apiVersion: v1\n"
                + "metadata:\n"
                + "  name: " + podname + "\n"
                + "  namespace: " + namespace + "\n"
                + "spec:\n"
                + "  containers:\n"
                + "    - name: " + podname + "\n"
                + "      resources:\n"
                + "        limits:\n"
                + "          memory: " + valueAsString + "\n"
                + "        requests:\n"
                + "          memory: " + valueAsString + "\n"
                + "\n";
        // @formatter:on
        log.debug(patch);
        try {
            this.pods().inNamespace(namespace).withName(podname).patch(patch);
        } catch (KubernetesClientException e) {
            // this typically happens when the feature gate InPlacePodVerticalScaling was not enabled
            if (e.toString().contains("Forbidden: pod updates may not change fields other than")) {
                log.error("Could not patch task. Please make sure that the feature gate 'InPlacePodVerticalScaling' is enabled in Kubernetes. See https://github.com/kubernetes/enhancements/issues/1287 for details. Task scaling will now be disabled for the rest of this workflow execution.");
            } else {
                log.error("Could not patch task: {}", t.getConfig().getName(), e);
            }
            throw new CannotPatchException( e.getMessage() );
        }
        List<Container> l = t.getPod().getSpec().getContainers();
        for (Container c : l) {
            if ( c.getName() == null || !c.getName().equals(podname) ) {
                continue;
            }
            ResourceRequirements req = c.getResources();
            Map<String, Quantity> limits = req.getLimits();
            limits.replace("memory", new Quantity(valueAsString));
            Map<String, Quantity> requests = req.getRequests();
            requests.replace("memory", new Quantity(valueAsString));
            log.info("container: {}", req);
        }
        return true;
    }

}
