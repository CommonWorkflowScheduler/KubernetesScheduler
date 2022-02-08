package fonda.scheduler.client;

import fonda.scheduler.model.NodeWithAlloc;
import fonda.scheduler.model.PodWithAge;
import fonda.scheduler.scheduler.Scheduler;
import io.fabric8.kubernetes.api.model.ContainerStatus;
import io.fabric8.kubernetes.api.model.Node;
import io.fabric8.kubernetes.api.model.Pod;
import io.fabric8.kubernetes.client.DefaultKubernetesClient;
import io.fabric8.kubernetes.client.Watcher;
import io.fabric8.kubernetes.client.WatcherException;
import io.fabric8.kubernetes.client.dsl.base.OperationContext;
import lombok.extern.slf4j.Slf4j;

import java.util.*;

@Slf4j
public class KubernetesClient extends DefaultKubernetesClient  {

    private final Map<String, NodeWithAlloc> nodeHolder= new HashMap<>();
    private final List<Scheduler> schedulerList = new LinkedList<>();
    private OperationContext operationContext;


    public KubernetesClient(){
        this.operationContext = new OperationContext();
        for( Node node : this.nodes().list().getItems() ){
            nodeHolder.put( node.getMetadata().getName(), new NodeWithAlloc(node) );
        }
        this.pods().inAnyNamespace().watch( new PodWatcher( this ) );
        this.nodes().watch( new NodeWatcher( this ) );
    }

    public void addScheduler( Scheduler scheduler ){
        log.info("Added scheduler {}", scheduler.getName());
        synchronized ( schedulerList ){
            schedulerList.add( scheduler );
        }
    }

    public void removeScheduler( Scheduler scheduler ){
        log.info("Removed scheduler {}", scheduler.getName());
        synchronized ( schedulerList ){
            schedulerList.remove( scheduler );
        }
    }

    private void informAllScheduler(){
        synchronized ( schedulerList ){
            for (Scheduler scheduler : schedulerList) {
                scheduler.informResourceChange();
            }
        }
    }

    private void informAllSchedulersNewNode( NodeWithAlloc node ){
        synchronized ( schedulerList ){
            for (Scheduler scheduler : schedulerList) {
                scheduler.newNode( node );
            }
        }
    }

    private void informAllSchedulersRemovedNode( NodeWithAlloc node ){
        synchronized ( schedulerList ){
            for (Scheduler scheduler : schedulerList) {
                scheduler.removedNode( node );
            }
        }
    }

    public List<NodeWithAlloc> getAllNodes(){
        return new ArrayList<>(this.nodeHolder.values());
    }

    class NodeWatcher implements Watcher<Node>{

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
                            processedNode = new NodeWithAlloc(node);
                            kubernetesClient.nodeHolder.put( node.getMetadata().getName(), processedNode );
                            change = true;
                        }
                    }
                    if ( change ) kubernetesClient.informAllSchedulersNewNode( processedNode );
                    break;
                case DELETED:
                    log.info("Node {} was deleted", node.getMetadata().getName());
                    synchronized ( kubernetesClient.nodeHolder ){
                        if ( kubernetesClient.nodeHolder.containsKey( node.getMetadata().getName() ) ){
                            processedNode  = kubernetesClient.nodeHolder.remove( node.getMetadata().getName() );
                            change = true;
                        }
                    }
                    if ( change ) kubernetesClient.informAllSchedulersRemovedNode( processedNode );
                    break;
                case ERROR:
                    log.info("Node {} has an error", node.getMetadata().getName());
                    //todo deal with error
                    break;
                case MODIFIED:
                    log.info("Node {} was an modified", node.getMetadata().getName());
                    //todo deal with changed stae
                    break;
            }
        }

        @Override
        public void onClose(WatcherException cause) {
            log.info( "Watcher was closed" );
        }
    }

    class PodWatcher implements Watcher<Pod> {

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
                        node.addPod( new PodWithAge(pod) ); break;
                    case MODIFIED:
                        final List<ContainerStatus> containerStatuses = pod.getStatus().getContainerStatuses();
                        if ( containerStatuses.size() == 0
                                ||
                                containerStatuses.get(0).getState().getTerminated() == null ) {
                            break;
                        }
                    case DELETED:
                    case ERROR:
                        log.info("Pod has released its resources: {}", pod.getMetadata().getName());
                        //Delete Pod in any case
                        node.removePod( pod );
                        kubernetesClient.informAllScheduler();
                        break;
                }

            }
        }


        @Override
        public void onClose(WatcherException cause) {
            log.info( "Watcher was closed" );
        }

    }

}
