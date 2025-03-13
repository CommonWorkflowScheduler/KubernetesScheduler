package cws.k8s.scheduler.client;

import cws.k8s.scheduler.model.NodeWithAlloc;
import cws.k8s.scheduler.model.PodWithAge;
import cws.k8s.scheduler.model.Task;
import cws.k8s.scheduler.util.MyExecListner;
import io.fabric8.kubernetes.api.model.*;
import io.fabric8.kubernetes.client.Config;
import io.fabric8.kubernetes.client.*;
import io.fabric8.kubernetes.client.dsl.*;
import lombok.extern.slf4j.Slf4j;

import java.io.ByteArrayOutputStream;
import java.math.BigDecimal;
import java.math.RoundingMode;
import java.util.*;

@Slf4j
public class CWSKubernetesClient {

    private final KubernetesClient client;

    private final Map<String, NodeWithAlloc> nodeHolder= new HashMap<>();
    private final List<Informable> informables = new LinkedList<>();

    public CWSKubernetesClient() {
        KubernetesClientBuilder builder = new KubernetesClientBuilder();
        this.client = builder.build();
        for( Node node : this.nodes().list().getItems() ){
            nodeHolder.put( node.getMetadata().getName(), new NodeWithAlloc(node,this) );
        }
        this.pods().inAnyNamespace().watch( new PodWatcher( this ) );
        this.nodes().watch( new NodeWatcher( this ) );
    }

    public Pod getPodByIp( String ip ) {
        if ( ip == null ) {
            throw new IllegalArgumentException("IP cannot be null");
        }
        return this.pods()
                .inAnyNamespace()
                .list()
                .getItems()
                .parallelStream()
                .filter( pod -> ip.equals( pod.getStatus().getPodIP() ) )
                .findFirst()
                .orElseGet( () -> {
                    log.warn("No Pod found for IP: {}", ip);
                    return null;
                });
    }

    public NonNamespaceOperation<Node, NodeList, Resource<Node>> nodes() {
        return client.nodes();
    }

    public MixedOperation<Pod, PodList, PodResource> pods() {
        return client.pods();
    }

    public Config getConfiguration() {
        return client.getConfiguration();
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
                client.bindings()
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
        final Quantity memory = client
                .top()
                .nodes()
                .metrics(node.getName())
                .getUsage()
                .get("memory");
        return Quantity.getAmountInBytes(memory);
    }

    private void forceDeletePod( Pod pod ) {
        this.pods()
                .inNamespace(pod.getMetadata().getNamespace())
                .withName(pod.getMetadata().getName())
                .withGracePeriod(0)
                .withPropagationPolicy( DeletionPropagation.BACKGROUND )
                .delete();
    }

    private void createPod( Pod pod ) {
        this.pods()
                .inNamespace(pod.getMetadata().getNamespace())
                .resource( pod )
                .create();
    }

    public void assignPodToNodeAndRemoveInit( PodWithAge pod, String node ) {
        if ( pod.getSpec().getInitContainers().size() > 0 ) {
            pod.getSpec().getInitContainers().remove( 0 );
        }
        pod.getMetadata().setResourceVersion( null );
        pod.getMetadata().setManagedFields( null );
        pod.getSpec().setNodeName( node );

        forceDeletePod( pod );
        createPod( pod );
    }

    public void execCommand( String podName, String namespace, String[] command, MyExecListner listener ){
        ByteArrayOutputStream out = new ByteArrayOutputStream();
        ByteArrayOutputStream error = new ByteArrayOutputStream();
        final ExecWatch exec = this.pods()
                .inNamespace( namespace )
                .withName( podName )
                .writingOutput( out )
                .writingError( error )
                .usingListener( listener )
                .exec( command );
        listener.setExec( exec );
        listener.setError( error );
        listener.setOut( out );
    }

    static class NodeWatcher implements Watcher<Node>{

        private final CWSKubernetesClient kubernetesClient;

        public NodeWatcher(CWSKubernetesClient kubernetesClient) {
            this.kubernetesClient = kubernetesClient;
        }

        @Override
        public void eventReceived( Watcher.Action action, Node node) {
            boolean change = false;
            NodeWithAlloc processedNode = null;
            switch (action) {
                case MODIFIED:
                    final NodeWithAlloc nodeWithAlloc = kubernetesClient.nodeHolder.get( node.getMetadata().getName() );
                    if ( nodeWithAlloc != null ){
                        nodeWithAlloc.update( node );
                        break;
                    }
                    // If the node is not in the nodeHolder, it is a new node
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
                default: log.warn("No implementation for {}", action);
            }
        }

        @Override
        public void onClose(WatcherException cause) {
            log.info( "Watcher was closed" );
        }
    }

    static class PodWatcher implements Watcher<Pod> {

        private final CWSKubernetesClient kubernetesClient;

        public PodWatcher(CWSKubernetesClient kubernetesClient) {
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

    public boolean inPlacePodVerticalScalingActive() {
        return featureGateActive("InPlacePodVerticalScaling");
    }

    public boolean featureGateActive( String featureGate ){
        return pods()
                .inNamespace( "kube-system" )
                .list()
                .getItems()
                .stream()
                .filter( p -> p.getMetadata().getName().startsWith( "kube-apiserver" ) )
                .anyMatch( p -> p
                        .getSpec()
                        .getContainers()
                        .stream()
                        .anyMatch( c -> c
                                .getCommand()
                                .contains( "--feature-gates=" + featureGate + "=true" )
                        )
                );
    }

    /**
     * It will create a patch for the memory limits and request values and submit it
     * to the cluster.
     * Moreover, it updates the task with the new pod.
     *
     * @param t          the task to be patched
     * @return false if patching failed because of InPlacePodVerticalScaling
     */
    public boolean patchTaskMemory( Task t ) {
        try {
            final String valueAsString = t.getPlanedRequirements().getRam()
                    .divide( BigDecimal.valueOf( 1024L * 1024L ) )
                    .setScale( 0, RoundingMode.CEILING ).toPlainString() + "Mi";
            final PodWithAge pod = t.getPod();
            String namespace = pod.getMetadata().getNamespace();
            String podname = pod.getName();
            Resource<Pod> podResource = pods()
                    .inNamespace( namespace )
                    .withName( podname );
            Container container = podResource.get().getSpec().getContainers().get(0); // Assuming only one container
            Container modifiedContainer = new ContainerBuilder(container)
                    .editOrNewResources()
                    .removeFromLimits( "memory" )
                    .removeFromRequests( "memory" )
                    .addToLimits("memory", new Quantity(valueAsString))
                    .addToRequests("memory", new Quantity(valueAsString))
                    .endResources()
                    .build();

            Pod modifiedPod = new PodBuilder( podResource.get() )
                    .editOrNewSpec()
                    .removeFromContainers( container )
                    .addToContainers(modifiedContainer)
                    .endSpec()
                    .editOrNewMetadata()
                    .addToLabels( "commonworkflowscheduler/memoryscaled", "true" )
                    .endMetadata()
                    .build();

            t.setPod( new PodWithAge( modifiedPod ) );

            podResource.patch(modifiedPod);

        } catch ( KubernetesClientException e ) {
            // this typically happens when the feature gate InPlacePodVerticalScaling was not enabled
            if (e.toString().contains("Forbidden: pod updates may not change fields other than")) {
                log.error("Could not patch task. Please make sure that the feature gate 'InPlacePodVerticalScaling' is enabled in Kubernetes. See https://github.com/kubernetes/enhancements/issues/1287 for details. Task scaling will now be disabled for the rest of this workflow execution.");
            } else {
                log.error("Could not patch task: {}", t.getConfig().getName(), e);
            }
            throw new CannotPatchException( e.getMessage() );
        }
        return true;
    }

}
