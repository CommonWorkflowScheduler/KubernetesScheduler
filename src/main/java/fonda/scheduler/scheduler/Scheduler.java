package fonda.scheduler.scheduler;

import fonda.scheduler.client.KubernetesClient;
import fonda.scheduler.model.NodeWithAlloc;
import fonda.scheduler.model.PodWithAge;
import fonda.scheduler.model.TaskConfig;
import io.fabric8.kubernetes.api.model.Binding;
import io.fabric8.kubernetes.api.model.ObjectMeta;
import io.fabric8.kubernetes.api.model.ObjectReference;
import io.fabric8.kubernetes.api.model.Pod;
import io.fabric8.kubernetes.client.Watch;
import io.fabric8.kubernetes.client.Watcher;
import io.fabric8.kubernetes.client.WatcherException;
import io.fabric8.kubernetes.client.dsl.PodResource;
import lombok.Getter;
import lombok.extern.slf4j.Slf4j;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

@Slf4j
public abstract class Scheduler {

    //Visible variables
    @Getter
    private final String name;
    @Getter
    private boolean close;

    final KubernetesClient client;
    private final Map<String, Pod> podsByUID = new HashMap<>();
    private final String namespace;
    final private List<Pod> unscheduledPods = new ArrayList<>(100);


    final Map<String, Map<String, TaskConfig>> taskConfigs = new HashMap<>();
    final Map<String, TaskConfig> taskConfigsByHash = new HashMap<>();


    private final SchedulerThread schedulerThread;

    private final Watch watcher;

    Scheduler(String name, KubernetesClient client, String namespace){
        this.name = System.getenv( "SCHEDULER_NAME" ) + "-" + name;
        this.namespace = namespace;
        log.trace( "Register scheduler for " + this.name );
        this.client = client;

        PodWatcher podWatcher = new PodWatcher(this);
        log.info("Start watching");
        watcher = client.pods().inNamespace( this.namespace ).watch(podWatcher);
        log.info("Watching");

        schedulerThread = new SchedulerThread( unscheduledPods,this );
        schedulerThread.start();
    }

    public abstract void schedule( final List<Pod> unscheduledPods );
    void podEventReceived(Watcher.Action action, Pod pod){}
    void onPodTermination( Pod pod ){
        //todo run in thread
    }

    /**
     * You may extend this method
     * @param pod
     * @param node
     * @return
     */
    boolean canPodBeScheduled( Pod pod, NodeWithAlloc node ){
        return node.canSchedule( pod );
    }

    void assignPodToNode( Pod pod, NodeWithAlloc node ){

        node.addPod( pod );

        log.info ( "Assign pod: " + pod.getMetadata().getName() + " to node: " + node.getMetadata().getName() );

        Binding b1 = new Binding();

        ObjectMeta om = new ObjectMeta();
        om.setName(pod.getMetadata().getName());
        om.setNamespace(pod.getMetadata().getNamespace());
        b1.setMetadata(om);

        ObjectReference objectReference = new ObjectReference();
        objectReference.setApiVersion("v1");
        objectReference.setKind("Node");
        objectReference.setName(node.getMetadata().getName());

        b1.setTarget(objectReference);

        client.bindings().create(b1);

        pod.getSpec().setNodeName( node.getMetadata().getName() );
        log.info ( "Assigned pod to:" + pod.getSpec().getNodeName());
    }

    /**
     * Close used resources
     */
    public void close(){
        watcher.close();
        schedulerThread.interrupt();
        this.close = true;
    }

    public void addUnscheduledPod( Pod pod ) {
        synchronized ( unscheduledPods ){
            unscheduledPods.add( pod );
            unscheduledPods.notifyAll();
        }
    }

    public void scheduledPod( Pod pod ) {
        synchronized ( unscheduledPods ){
            unscheduledPods.remove( pod );
        }
    }

    public void informResourceChange() {
        synchronized ( unscheduledPods ){
            unscheduledPods.notifyAll();
        }
    }


    public void addPod( Pod pod ) {
        synchronized (podsByUID){
            podsByUID.put( pod.getMetadata().getUid(), pod );
        }
    }

    public void removePod( Pod pod ) {
        synchronized (podsByUID){
            podsByUID.remove( pod.getMetadata().getUid() );
        }
    }

    List<NodeWithAlloc> getNodeList(){
        return client.getAllNodes();
    }

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

    public void newNode(NodeWithAlloc node) {
        informResourceChange();
    }

    public void removedNode(NodeWithAlloc node) {}


    static class PodWatcher implements Watcher<Pod> {

        private final Scheduler scheduler;

        public PodWatcher(Scheduler scheduler) {
            this.scheduler = scheduler;
        }

        @Override
        public void eventReceived(Action action, Pod pod) {

            scheduler.podEventReceived(action, pod);

            if (!scheduler.name.equals(pod.getSpec().getSchedulerName())) return;

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
                    scheduler.addPod(pwa);
                    if (pwa.getSpec().getNodeName() == null && pwa.getSpec().getSchedulerName().equalsIgnoreCase( scheduler.name )) {
                        scheduler.addUnscheduledPod ( pwa );
                    }
                    break;
                case MODIFIED:
                    if (pod.getStatus().getContainerStatuses().size() > 0 && pod.getStatus().getContainerStatuses().get(0).getState().getTerminated() != null) {
                        scheduler.onPodTermination(pwa);
                    }
                    break;
                case DELETED:
                    scheduler.removePod(pwa);
            }

        }


        @Override
        public void onClose(WatcherException cause) {
            log.info( "Watcher was closed" );
        }

    }


}
