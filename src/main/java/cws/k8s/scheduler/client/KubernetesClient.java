package cws.k8s.scheduler.client;

import cws.k8s.scheduler.model.NodeWithAlloc;
import cws.k8s.scheduler.model.PodWithAge;
import cws.k8s.scheduler.util.MyExecListner;
import io.fabric8.kubernetes.api.model.*;
import io.fabric8.kubernetes.client.DefaultKubernetesClient;
import io.fabric8.kubernetes.client.KubernetesClientException;
import io.fabric8.kubernetes.client.Watcher;
import io.fabric8.kubernetes.client.dsl.ExecWatch;
import io.fabric8.kubernetes.client.informers.ResourceEventHandler;
import io.fabric8.kubernetes.client.informers.SharedIndexInformer;
import lombok.extern.slf4j.Slf4j;

import java.io.ByteArrayOutputStream;
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
        final SharedIndexInformer<Pod> podSharedIndexInformer = this.pods().inAnyNamespace().inform( new PodHandler( this ) );
        podSharedIndexInformer.start();
        final SharedIndexInformer<Node> nodeSharedIndexInformer = this.nodes().inform( new NodeHandler( this ) );
        nodeSharedIndexInformer.start();
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

    static class NodeHandler implements ResourceEventHandler<Node>{

        private final KubernetesClient kubernetesClient;

        public NodeHandler(KubernetesClient kubernetesClient) {
            this.kubernetesClient = kubernetesClient;
        }

        @Override
        public void onAdd( Node node ) {
            boolean change = false;
            NodeWithAlloc processedNode = null;
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
        }

        @Override
        public void onUpdate( Node oldObj, Node newObj ) {
            //todo deal with changed state
        }

        @Override
        public void onDelete( Node node, boolean deletedFinalStateUnknown ) {
            boolean change = false;
            NodeWithAlloc processedNode = null;
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
        }

    }

    static class PodHandler implements ResourceEventHandler<Pod> {

        private final KubernetesClient kubernetesClient;

        public PodHandler(KubernetesClient kubernetesClient) {
            this.kubernetesClient = kubernetesClient;
        }

        public void eventReceived( Watcher.Action action, Pod pod) {
            String nodeName = pod.getSpec().getNodeName();
            if( nodeName != null ){
                NodeWithAlloc node = kubernetesClient.nodeHolder.get( pod.getSpec().getNodeName() );
                switch ( action ){
                    case ADDED:
                        if ( !PodWithAge.hasFinishedOrFailed( pod ) ) {
                            node.addPod(new PodWithAge(pod), false);
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
        public void onAdd( Pod pod ) {
            eventReceived( Watcher.Action.ADDED, pod );
        }

        @Override
        public void onUpdate( Pod oldPod, Pod newPod ) {
            eventReceived( Watcher.Action.MODIFIED, newPod );
        }

        @Override
        public void onDelete( Pod pod, boolean deletedFinalStateUnknown ) {
            eventReceived( Watcher.Action.DELETED, pod );
        }

    }

}
