package fonda.scheduler.scheduler;

import fonda.scheduler.model.PodListWithIndex;
import fonda.scheduler.model.PodWithAge;
import fonda.scheduler.model.TaskConfig;
import io.fabric8.kubernetes.api.model.*;
import io.fabric8.kubernetes.client.KubernetesClient;
import io.fabric8.kubernetes.client.Watch;
import io.fabric8.kubernetes.client.Watcher;
import io.fabric8.kubernetes.client.WatcherException;
import io.fabric8.kubernetes.client.dsl.PodResource;
import io.fabric8.kubernetes.client.dsl.base.OperationContext;
import io.fabric8.kubernetes.client.informers.ListerWatcher;
import io.fabric8.kubernetes.client.informers.ResourceEventHandler;
import io.fabric8.kubernetes.client.informers.SharedInformerEventListener;
import io.fabric8.kubernetes.client.informers.impl.DefaultSharedIndexInformer;
import lombok.Getter;
import lombok.extern.slf4j.Slf4j;

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

@Slf4j
public abstract class Scheduler {

    DefaultSharedIndexInformer<Node, NodeList> defaultSharedIndexInformerNode;
    final KubernetesClient client;
    private OperationContext operationContext;
    private ConcurrentLinkedQueue<SharedInformerEventListener> workerQueueNode;
    PodListWithIndex podList = new PodListWithIndex();
    @Getter
    private final String name;

    final Map<String, Map<String, TaskConfig>> taskConfigs = new HashMap<>();
    final Map<String, TaskConfig> taskConfigsByHash = new HashMap<>();
    @Getter
    private boolean close;

    final static private ExecutorService executorService = Executors.newFixedThreadPool(30);

    private final Watch watcher;

    Scheduler( String name, KubernetesClient client ){
        this.name = System.getenv( "SCHEDULER_NAME" ) + "-" + name;
        log.trace( "Register scheduler for " + this.name );

        this.client = client;

        this.workerQueueNode = new ConcurrentLinkedQueue<>();

        this.operationContext = new OperationContext();


        setUpIndexInformerNode();

        try {
            Thread.sleep(5000);
        } catch (InterruptedException e) {
            e.printStackTrace();
        }

        watcher =  client.pods().watch(new Watcher<>() {
            @Override
            public void eventReceived(Action action, Pod pod) {

                podEventReceived(action, pod);

                if (!Scheduler.this.name.equals(pod.getSpec().getSchedulerName())) return;

                PodWithAge pwa = new PodWithAge(pod);
                if (pod.getMetadata().getLabels() != null) {
                    log.debug("Got pod: " + pod.getMetadata().getName() +
                            " app: " + pod.getMetadata().getLabels().getOrDefault("app", "-") +
                            " processName: " + pod.getMetadata().getLabels().getOrDefault("processName", "-") +
                            " runName: " + pod.getMetadata().getLabels().getOrDefault("runName", "-") +
                            " taskName: " + pod.getMetadata().getLabels().getOrDefault("taskName", "-") +
                            " scheduler: " + pwa.getSpec().getSchedulerName() +
                            " action: " + action
                    );
                } else {
                    log.debug("Got pod " + pod.getMetadata().getName() + " scheduler: " + pwa.getSpec().getSchedulerName());
                }

                switch (action) {
                    case ADDED:
                        addPod(pwa);
                        if (pwa.getSpec().getNodeName() == null) {
                            executorService.submit(() -> schedule(pwa));
                        }
                        break;
                    case MODIFIED:
                        if (pod.getStatus().getContainerStatuses().size() > 0 && pod.getStatus().getContainerStatuses().get(0).getState().getTerminated() != null) {
                            executorService.submit(() -> onPodTermination(pwa));
                        }
                        break;
                    case DELETED:
                        removePod(pwa);
                        executorService.submit(() -> schedule(null));
                }

            }

            @Override
            public void onClose(WatcherException cause) {
                log.info( "Watcher was closed" );
            }

        });

    }

    void podEventReceived(Watcher.Action action, Pod pod){}

    void onPodTermination( Pod pod ){}

    /**
     *
     */
    private void setUpIndexInformerNode() {
        ListerWatcher listerWatcher = new ListerWatcher() {
            @Override
            public Watch watch(ListOptions params, String namespace, OperationContext context, Watcher watcher) {
                return client.nodes().watch(new Watcher<>() {
                    @Override
                    public void eventReceived(Action action, Node resource) {

                    }

                    @Override
                    public void onClose(WatcherException cause) {

                    }
                });
            }

            @Override
            public Object list(ListOptions params, String namespace, OperationContext context) {
                return getNodeList();
            }
        };

        defaultSharedIndexInformerNode = new DefaultSharedIndexInformer(Node.class, listerWatcher, 600000, operationContext, workerQueueNode);

        try {
            defaultSharedIndexInformerNode.run();
        } catch (Exception e) {
            e.printStackTrace();
        }

        defaultSharedIndexInformerNode.addEventHandler(new ResourceEventHandler<>() {
            @Override
            public void onAdd(Node obj) {

            }

            @Override
            public void onUpdate(Node oldObj, Node newObj) {

            }

            @Override
            public void onDelete(Node obj, boolean deletedFinalStateUnknown) {

            }
        });
    }

    void assignPodToNode( Pod pod, Node node ){
        log.info ( "Assign pod: " + pod.getMetadata().getName() + " to node: " + node );

        Binding b1 = new Binding();

        ObjectMeta om = new ObjectMeta();
        om.setName(pod.getMetadata().getName());
        om.setNamespace(pod.getMetadata().getNamespace());
        b1.setMetadata(om);

        ObjectReference objectReference = new ObjectReference();
        objectReference.setApiVersion("v1");
        objectReference.setKind("Node");
        objectReference.setName(node.getMetadata().getName()); //hier ersetzen durch tatsächliche Node

        b1.setTarget(objectReference);

        client.bindings().create(b1);

        pod.getSpec().setNodeName( node.getMetadata().getName() ); // Hier stimmt evtl etwas nicht
        log.info ( "Assigned pod to:" + pod.getSpec().getNodeName());
    }

    /**
     * Close used resources
     */
    public void close(){
        watcher.close();
        this.close = true;
    }


    public void addPod( Pod pod ) {
        podList.addPodToList( pod );
    }

    public void removePod( Pod pod ) {
        podList.removePodFromList( pod );
    }

    NodeList getNodeList(){
        return client.nodes().list();
    }

    public abstract void schedule( Pod podToSchedule );

    String getWorkingDir( Pod pod ){
        return pod.getSpec().getContainers().get(0).getWorkingDir();
    }

    PodResource<Pod> findPodByName(String name ){
        return client.pods().withName( name );
    }

    public void addTaskConfig( String name, TaskConfig config ) {

        Map< String, TaskConfig> conf;
        synchronized (taskConfigs) {
            if( taskConfigs.containsKey( name ) ){
                conf = taskConfigs.get( name );
            } else {
                conf = new HashMap<>();
                taskConfigs.put( name, conf );
            }
        }
        synchronized ( conf ){
            conf.put( config.getName(), config );
        }
        synchronized ( taskConfigsByHash ){
            taskConfigsByHash.put ( config.getHash(), config );
        }
    }

    TaskConfig getConfigFor(String taskname, String name ){
        return taskConfigs.get( taskname ).get( name );
    }

    TaskConfig getConfigFor(String hash ){
        return taskConfigsByHash.get( hash );
    }

    public Map<String, Object> getSchedulerParams( String taskname, String name ){
        TaskConfig config = getConfigFor(taskname, name);
        Map<String, Object> result = new HashMap();
        config.getSchedulerParams().entrySet().forEach( x -> result.put( x.getKey(), x.getValue().get(0)) );
        return result;
    }

}
