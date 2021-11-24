package fonda.scheduler.scheduler;

import fonda.scheduler.client.KubernetesClient;
import fonda.scheduler.model.*;
import fonda.scheduler.scheduler.util.NodeTaskAlignment;
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
    private final String execution;
    @Getter
    private final String namespace;
    @Getter
    private final String dns;
    @Getter
    private boolean close;

    final KubernetesClient client;
    final private List<Task> unscheduledTasks = new ArrayList<>(100);
    final private List<Task> unfinishedTasks = new ArrayList<>(100);
    final Map<String, Task> tasksByHash = new HashMap<>();
    private final Watch watcher;
    private final TaskprocessingThread schedulingThread;
    private final TaskprocessingThread finishThread;

    Scheduler(String execution, KubernetesClient client, String namespace, SchedulerConfig config){
        this.execution = execution;
        this.name = System.getenv( "SCHEDULER_NAME" ) + "-" + execution;
        this.namespace = namespace;
        log.trace( "Register scheduler for " + this.name );
        this.client = client;
        this.dns = config.dns;

        PodWatcher podWatcher = new PodWatcher(this);

        schedulingThread = new TaskprocessingThread( unscheduledTasks, this::schedule );
        schedulingThread.start();

        finishThread = new TaskprocessingThread(unfinishedTasks, this::terminateTasks );
        finishThread.start();

        log.info("Start watching");
        watcher = client.pods().inNamespace( this.namespace ).watch(podWatcher);
        log.info("Watching");
    }

    /* Abstract methods */

    /**
     *
     * @param unscheduledTasks
     * @return the number of unscheduled Tasks
     */
    public int schedule( final List<Task> unscheduledTasks ) {
        final List<NodeTaskAlignment> taskNodeAlignment = getTaskNodeAlignment(unscheduledTasks);
        //check if still possible...
        boolean possible = true;
        if (!possible) return taskNodeAlignment.size();

        for (NodeTaskAlignment nodeTaskAlignment : taskNodeAlignment) {
            if (isClose()) return -1;
            assignTaskToNode( nodeTaskAlignment );
            taskWasScheduled(nodeTaskAlignment.task);

        }
        return unscheduledTasks.size() - taskNodeAlignment.size();
    }

    abstract List<NodeTaskAlignment> getTaskNodeAlignment( final List<Task> unscheduledTasks );

    abstract int terminateTasks( final List<Task> finishedTasks );

    /* Pod Event */

    void podEventReceived(Watcher.Action action, Pod pod){}

    void onPodTermination( Pod pod ){
        Task t = changeStateOfTask( pod, State.PROCESSING_FINISHED );

        //If null, task was already changed
        if( t == null ) return;

        synchronized (unfinishedTasks){
            unfinishedTasks.add( t );
            unfinishedTasks.notifyAll();
        }
    }

    void taskWasFinished( Task task ){
        synchronized (unfinishedTasks){
            unfinishedTasks.remove( task );
        }
        task.getState().setState(State.FINISHED);
    }

    public void schedulePod(Pod pod ) {
        Task t = changeStateOfTask( pod, State.UNSCHEDULED );
        //If null, task was already unscheduled
        if ( t == null ) return;
        t.setPod( pod );
        synchronized (unscheduledTasks){
            unscheduledTasks.add( t );
            unscheduledTasks.notifyAll();
        }
    }

    void taskWasScheduled(Task task ) {
        synchronized (unscheduledTasks){
            unscheduledTasks.remove( task );
        }
        task.getState().setState( State.SCHEDULED );
    }

    public void markPodAsDeleted( Pod pod ) {
        changeStateOfTask( pod, State.DELETED );
    }

    /* External access to Tasks */

    public void addTask( TaskConfig conf ) {
        synchronized (tasksByHash) {
            if( ! tasksByHash.containsKey( conf.getHash() ) ){
                tasksByHash.put( conf.getHash(), new Task(conf) );
            }
        }
    }

    TaskConfig getConfigFor( String hash ){
        synchronized (tasksByHash) {
            if( tasksByHash.containsKey( hash ) ){
                return tasksByHash.get( hash ).getConfig();
            }
        }
        return null;
    }

    public Map<String, Object> getSchedulerParams( String taskname, String name ){
        Map<String, Object> result = new HashMap();
        return result;
    }

    public TaskState getTaskState(String taskid) {
        synchronized (tasksByHash) {
            if( tasksByHash.containsKey( taskid ) ){
                return tasksByHash.get( taskid ).getState();
            }
        }
        return null;
    }

    /* Nodes */

    public void newNode(NodeWithAlloc node) {
        informResourceChange();
    }

    public void removedNode(NodeWithAlloc node) {}

    List<NodeWithAlloc> getNodeList(){
        return client.getAllNodes();
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

    void assignTaskToNode( NodeTaskAlignment alignment ){

        alignment.task.setNode( alignment.node.getNodeLocation() );

        final Pod pod = alignment.task.getPod();

        alignment.node.addPod( pod );

        log.info ( "Assign pod: " + pod.getMetadata().getName() + " to node: " + alignment.node.getMetadata().getName() );

        Binding b1 = new Binding();

        ObjectMeta om = new ObjectMeta();
        om.setName(pod.getMetadata().getName());
        om.setNamespace(pod.getMetadata().getNamespace());
        b1.setMetadata(om);

        ObjectReference objectReference = new ObjectReference();
        objectReference.setApiVersion("v1");
        objectReference.setKind("Node");
        objectReference.setName(alignment.node.getMetadata().getName());

        b1.setTarget(objectReference);

        client.bindings().create(b1);

        pod.getSpec().setNodeName( alignment.node.getMetadata().getName() );
        log.info ( "Assigned pod to:" + pod.getSpec().getNodeName());
    }

    /* Helper */

    /**
     *
     * @param pod
     * @param state
     * @return returns the task, if the state was changed
     */
    private Task changeStateOfTask( Pod pod, State state ){
        Task t = getTaskByPod( pod );
        if( t != null ){
            synchronized ( t.getState() ){
                if( t.getState().getState() != state ){
                    t.getState().setState( state );
                    return t;
                } else {
                    return null;
                }
            }
        }
        return null;
    }

    @Deprecated
    PodResource<Pod> findPodByName(String name ){
        return client.pods().withName( name );
    }
    
    public void informResourceChange() {
        synchronized (unscheduledTasks){
            unscheduledTasks.notifyAll();
        }
    }

    private Task getTaskByPod( Pod pod ) {
        Task t = null;
        synchronized (tasksByHash) {
            if( tasksByHash.containsKey( pod.getMetadata().getName() ) ){
                t = tasksByHash.get( pod.getMetadata().getName() );
            }
        }

        if ( t == null ){
            throw new IllegalStateException( "No task with config found for: " + pod.getMetadata().getName() );
        }

        return t;
    }

    /**
     * Close used resources
     */
    public void close(){
        watcher.close();
        schedulingThread.interrupt();
        this.close = true;
    }

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
                    if (pwa.getSpec().getNodeName() == null && pwa.getSpec().getSchedulerName().equalsIgnoreCase( scheduler.name )) {
                        scheduler.schedulePod( pwa );
                    }
                    break;
                case MODIFIED:
                    if (pod.getStatus().getContainerStatuses().size() > 0 && pod.getStatus().getContainerStatuses().get(0).getState().getTerminated() != null) {
                        scheduler.onPodTermination(pwa);
                    }
                    break;
                case DELETED:
                    scheduler.markPodAsDeleted(pwa);
            }

        }


        @Override
        public void onClose(WatcherException cause) {
            log.info( "Watcher was closed" );
        }

    }


}
