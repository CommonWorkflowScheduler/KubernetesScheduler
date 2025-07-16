package cws.k8s.scheduler.client;

import cws.k8s.scheduler.model.NodeWithAlloc;
import cws.k8s.scheduler.model.PodWithAge;
import cws.k8s.scheduler.model.Task;
import cws.k8s.scheduler.util.MyExecListner;
import io.fabric8.kubernetes.api.model.*;
import io.fabric8.kubernetes.client.*;
import io.fabric8.kubernetes.client.Config;
import io.fabric8.kubernetes.client.dsl.*;
import io.fabric8.kubernetes.client.informers.ResourceEventHandler;
import io.fabric8.kubernetes.client.informers.SharedIndexInformer;
import jakarta.annotation.PreDestroy;
import lombok.extern.slf4j.Slf4j;

import java.io.ByteArrayOutputStream;
import java.math.BigDecimal;
import java.math.RoundingMode;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CopyOnWriteArrayList;

@Slf4j
public class CWSKubernetesClient implements AutoCloseable {

    private final KubernetesClient client;

    private final Map<String, NodeWithAlloc> nodeHolder= new ConcurrentHashMap<>();
    private final List<Informable> informables = new CopyOnWriteArrayList<>();
    private final SharedIndexInformer<Pod> podInform;
    private final SharedIndexInformer<Node> nodeInform;

    public CWSKubernetesClient() {
        KubernetesClientBuilder builder = new KubernetesClientBuilder();
        this.client = builder.build();
        for( Node node : this.nodes().list().getItems() ){
            nodeHolder.put( node.getMetadata().getName(), new NodeWithAlloc(node,this) );
        }
        podInform = this.pods().inAnyNamespace().inform( new PodHandler( this ) );
        nodeInform = this.nodes().inform( new NodeHandler( this ) );
    }

    @PreDestroy
    @Override
    public void close() throws Exception {
        podInform.close();
        nodeInform.close();
        client.close();
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
        informables.add( informable );
    }

    public void removeInformable( Informable informable ){
        informables.remove( informable );
    }

    private void informAllInformable(){
        for (Informable informable : informables ) {
            informable.informResourceChange();
        }
    }

    private void informAllNewNode( NodeWithAlloc node ){
        for (Informable informable : informables ) {
            informable.newNode( node );
        }
    }

    private void informAllRemovedNode( NodeWithAlloc node ){
        for (Informable informable : informables ) {
            informable.removedNode( node );
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

    static class NodeHandler implements ResourceEventHandler<Node> {

        private final CWSKubernetesClient kubernetesClient;

        public NodeHandler(CWSKubernetesClient kubernetesClient) {
            this.kubernetesClient = kubernetesClient;
        }

        @Override
        public void onAdd(Node node) {
            kubernetesClient.nodeHolder.computeIfAbsent(
                    node.getMetadata().getName(),
                    name -> {
                        NodeWithAlloc newNode = new NodeWithAlloc(node, kubernetesClient);
                        log.info("New Node {} was added", name);
                        kubernetesClient.informAllNewNode(newNode);
                        return newNode;
                    }
            );
        }

        @Override
        public void onUpdate(Node oldNode, Node newNode) {
            final NodeWithAlloc nodeWithAlloc = kubernetesClient.nodeHolder.get( newNode.getMetadata().getName() );
            if ( nodeWithAlloc != null ){
                nodeWithAlloc.update( newNode );
            } else {
                onAdd( newNode );
            }
        }

        @Override
        public void onDelete(Node node, boolean deletedFinalStateUnknown) {
            NodeWithAlloc processedNode = kubernetesClient.nodeHolder.remove( node.getMetadata().getName() );
            if ( processedNode != null ) {
                log.info("Node {} was deleted", node.getMetadata().getName());
                kubernetesClient.informAllRemovedNode( processedNode );
            }
        }
    }

    static class PodHandler implements ResourceEventHandler<Pod> {

        private final CWSKubernetesClient kubernetesClient;

        public PodHandler( CWSKubernetesClient kubernetesClient) {
            this.kubernetesClient = kubernetesClient;
        }

        @Override
        public void onAdd(Pod pod) {
            String nodeName = pod.getSpec().getNodeName();
            if( nodeName != null ) {
                NodeWithAlloc node = kubernetesClient.nodeHolder.get( pod.getSpec().getNodeName() );
                if ( !PodWithAge.hasFinishedOrFailed( pod ) ) {
                    node.addPod(new PodWithAge(pod));
                } else {
                    // Pod is finished or failed, handle it
                    onDelete( pod, false);
                }
            }
        }

        @Override
        public void onUpdate(Pod oldPod, Pod newPod) {
            onAdd( newPod );
        }

        @Override
        public void onDelete(Pod pod, boolean deletedFinalStateUnknown) {
            String nodeName = pod.getSpec().getNodeName();
            if( nodeName != null ) {
                NodeWithAlloc node = kubernetesClient.nodeHolder.get( pod.getSpec().getNodeName() );
                if ( node.removePod( pod ) ){
                    log.info("Pod has released its resources: {}", pod.getMetadata().getName());
                    kubernetesClient.informAllInformable();
                }
            }
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

            podResource
                    .subresource( "resize" )
                    .patch(modifiedPod);

            podResource.edit(p -> new PodBuilder(p)
                    .editMetadata()
                    .addToLabels("commonworkflowscheduler/memoryscaled", "true")
                    .endMetadata()
                    .build());

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
