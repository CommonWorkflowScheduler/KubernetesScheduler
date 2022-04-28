package fonda.scheduler.scheduler;

import fonda.scheduler.client.KubernetesClient;
import fonda.scheduler.dag.DAG;
import fonda.scheduler.model.*;
import fonda.scheduler.util.Batch;
import fonda.scheduler.util.NodeTaskAlignment;
import io.fabric8.kubernetes.api.model.Binding;
import io.fabric8.kubernetes.api.model.ObjectMeta;
import io.fabric8.kubernetes.api.model.ObjectReference;
import io.fabric8.kubernetes.api.model.Pod;
import io.fabric8.kubernetes.client.Watch;
import io.fabric8.kubernetes.client.Watcher;
import io.fabric8.kubernetes.client.WatcherException;
import lombok.Getter;
import lombok.extern.slf4j.Slf4j;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.PrintWriter;
import java.util.*;

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
    @Getter
    private final DAG dag;

    private final Object batchHelper = new Object();
    private int currentBatch = 0;
    private Batch currentBatchInstance = null;

    final KubernetesClient client;
    private final Set<Task> upcomingTasks = new HashSet<>();
    private final List<Task> unscheduledTasks = new ArrayList<>(100);
    private final List<Task> unfinishedTasks = new ArrayList<>(100);
    final Map<String, Task> tasksByHash = new HashMap<>();
    private final Watch watcher;
    private final TaskprocessingThread schedulingThread;
    private final TaskprocessingThread finishThread;

    final boolean traceEnabled;

    Scheduler(String execution, KubernetesClient client, String namespace, SchedulerConfig config){
        this.execution = execution;
        this.name = System.getenv( "SCHEDULER_NAME" ) + "-" + execution;
        this.namespace = namespace;
        log.trace( "Register scheduler for " + this.name );
        this.client = client;
        this.dns = config.dns;
        this.dag = new DAG();
        this.traceEnabled = config.traceEnabled;

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
        final ScheduleObject scheduleObject = getTaskNodeAlignment(unscheduledTasks, getAvailableByNode());
        final List<NodeTaskAlignment> taskNodeAlignment = scheduleObject.getTaskAlignments();

        //check if still possible...
        if ( scheduleObject.isCheckStillPossible() ) {
            boolean possible = validSchedulePlan ( taskNodeAlignment );
            if (!possible) {
                log.info("The whole scheduling plan is not possible anymore.");
                return taskNodeAlignment.size();
            }
        }

        int failure = 0;
        for (NodeTaskAlignment nodeTaskAlignment : taskNodeAlignment) {
            try {
                if (isClose()) return -1;
                if ( !assignTaskToNode( nodeTaskAlignment ) ){
                    failure++;
                    continue;
                }
            } catch ( Exception e ){
                undoTaskScheduling( nodeTaskAlignment.task );
                continue;
            }
            taskWasScheduled(nodeTaskAlignment.task);
        }
        return unscheduledTasks.size() - taskNodeAlignment.size() + failure;
    }

    /**
     * Call this method in case of any scheduling problems
     * @param task
     */
    void undoTaskScheduling( Task task ){}


    public boolean validSchedulePlan( List<NodeTaskAlignment> taskNodeAlignment ){
        List<NodeWithAlloc> items = getNodeList();
        Map< NodeWithAlloc, Requirements> availableByNode = new HashMap<>();
        for ( NodeWithAlloc item : items ) {
            final Requirements availableResources = item.getAvailableResources();
            availableByNode.put(item, availableResources);
        }
        for ( NodeTaskAlignment nodeTaskAlignment : taskNodeAlignment ) {
            availableByNode.get(nodeTaskAlignment.node).subFromThis(nodeTaskAlignment.task.getPod().getRequest());
        }
        for ( Map.Entry<NodeWithAlloc, Requirements> e : availableByNode.entrySet() ) {
            if ( ! e.getValue().higherOrEquals( Requirements.ZERO ) ) return false;
        }
        return true;
    }

    abstract ScheduleObject getTaskNodeAlignment(
            final List<Task> unscheduledTasks,
            final Map<NodeWithAlloc, Requirements> availableByNode
    );

    int terminateTasks( final List<Task> finishedTasks ) {
        for (Task finishedTask : finishedTasks) {
            taskWasFinished( finishedTask );
        }
        return 0;
    }

    /* Pod Event */

    void podEventReceived(Watcher.Action action, Pod pod){}

    void onPodTermination( PodWithAge pod ){
        Task t = changeStateOfTask( pod, State.PROCESSING_FINISHED );

        //If null, task was already changed
        if( t == null ) return;
        t.setPod( pod );

        synchronized (unfinishedTasks){
            unfinishedTasks.add( t );
            unfinishedTasks.notifyAll();
        }
    }

    void taskWasFinished( Task task ){
        synchronized (unfinishedTasks){
            unfinishedTasks.remove( task );
        }
        task.getState().setState(task.wasSuccessfullyExecuted() ? State.FINISHED : State.FINISHED_WITH_ERROR);
    }

    public void schedulePod(PodWithAge pod ) {
        Task task = changeStateOfTask( pod, State.UNSCHEDULED );
        //If null, task was already unscheduled
        if ( task == null ) return;
        task.setPod( pod );
        if ( task.getBatch() == null ){
            synchronized (unscheduledTasks){
                unscheduledTasks.add( task );
                unscheduledTasks.notifyAll();
                synchronized ( upcomingTasks ){
                    upcomingTasks.remove( task );
                }
            }
        } else {
            Batch batch = task.getBatch();
            batch.informScheduable( task );
            tryToScheduleBatch( batch );
        }
    }

    private void tryToScheduleBatch( Batch batch ){
        if ( batch.canSchedule() ){
            synchronized (unscheduledTasks){
                final List<Task> tasksToScheduleAndDestroy = batch.getTasksToScheduleAndDestroy();
                unscheduledTasks.addAll(tasksToScheduleAndDestroy);
                unscheduledTasks.notifyAll();
                synchronized ( upcomingTasks ){
                    tasksToScheduleAndDestroy.forEach(upcomingTasks::remove);
                }
            }
        }
    }

    void taskWasScheduled(Task task ) {
        synchronized (unscheduledTasks){
            unscheduledTasks.remove( task );
        }
        taskWasScheduledSetState( task );
    }

    void taskWasScheduledSetState( Task task ){
        task.getState().setState( State.PREPARED );
    }

    public void markPodAsDeleted( PodWithAge pod ) {
        final Task task = changeStateOfTask(pod, State.DELETED);
        task.setPod( pod );
    }

    /* External access to Tasks */

    public void addTask( TaskConfig conf ) {
        final Task task = new Task( conf, dag );
        synchronized (tasksByHash) {
            if( ! tasksByHash.containsKey( conf.getHash() ) ){
                tasksByHash.put( conf.getHash(), task );
            }
        }
        synchronized ( upcomingTasks ){
            upcomingTasks.add( task );
        }
        if( currentBatchInstance != null ){
            currentBatchInstance.registerTask( task );
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

    /**
     * Chooses best param for a task
     * @param taskname
     * @param name
     * @return
     */
    public Map<String, Object> getSchedulerParams( String taskname, String name ){
        return new HashMap<>();
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

    /**
     * Checks if a node fulfills all requirements for a pod. This means: <br>
     * - enough resources available <br>
     * - Affinities match
     * @param availableByNode
     * @param pod
     * @param node
     * @return
     */
    boolean canSchedulePodOnNode(Requirements availableByNode, PodWithAge pod, NodeWithAlloc node ) {
        return availableByNode.higherOrEquals( pod.getRequest() ) && affinitiesMatch( pod, node );
    }

    boolean affinitiesMatch( PodWithAge pod, NodeWithAlloc node ){
        final Map<String, String> podsNodeSelector = pod.getSpec().getNodeSelector();
        final Map<String, String> nodesLabels = node.getMetadata().getLabels();
        if ( podsNodeSelector == null || podsNodeSelector.isEmpty() ) return true;
        //cannot be fulfilled if podsNodeSelector is not empty
        if ( nodesLabels == null || nodesLabels.isEmpty() ) return false;

        return nodesLabels.entrySet().containsAll( podsNodeSelector.entrySet() );
    }

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
    boolean canPodBeScheduled( PodWithAge pod, NodeWithAlloc node ){
        return node.canSchedule( pod );
    }

    boolean assignTaskToNode( NodeTaskAlignment alignment ){

        final File nodeFile = new File(alignment.task.getWorkingDir() + '/' + ".command.node");

        try(PrintWriter printWriter = new PrintWriter(nodeFile)){
            printWriter.println( alignment.node.getName() );
        } catch (FileNotFoundException e) {
            e.printStackTrace();
        }

        alignment.task.setNode( alignment.node.getNodeLocation() );

        final PodWithAge pod = alignment.task.getPod();

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

        return true;
    }

    /* Helper */

    public void startBatch(){
        synchronized (batchHelper){
            if ( currentBatchInstance == null || currentBatchInstance.isClosed() ){
                currentBatchInstance = new Batch( currentBatch++ );
            }
        }
    }

    public void endBatch( int tasksInBatch ){
        synchronized (batchHelper){
            currentBatchInstance.close( tasksInBatch );
            tryToScheduleBatch( currentBatchInstance );
        }
    }

    /**
     *
     * @param pod
     * @param state
     * @return returns the task, if the state was changed
     */
    Task changeStateOfTask(Pod pod, State state){
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

    public void informResourceChange() {
        synchronized (unscheduledTasks){
            unscheduledTasks.notifyAll();
        }
    }

    Task getTaskByPod( Pod pod ) {
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

    Map<NodeWithAlloc, Requirements> getAvailableByNode(){
        Map<NodeWithAlloc, Requirements> availableByNode = new HashMap<>();
        List<String> logInfo = new LinkedList<>();
        logInfo.add("------------------------------------");
        for (NodeWithAlloc item : getNodeList()) {
            final Requirements availableResources = item.getAvailableResources();
            availableByNode.put(item, availableResources);
            logInfo.add("Node: " + item.getName() + " " + availableResources);
        }
        logInfo.add("------------------------------------");
        log.info(String.join("\n", logInfo));
        return availableByNode;
    }

    /**
     * Filters all nodes, that have enough resources and fulfill the affinities
     * @param availableByNode
     * @param task
     * @return
     */
    Set<NodeWithAlloc> getMatchingNodesForTask(Map<NodeWithAlloc, Requirements> availableByNode, Task task ){
        Set<NodeWithAlloc> result = new HashSet<>();
        for (Map.Entry<NodeWithAlloc, Requirements> entry : availableByNode.entrySet()) {
            if ( this.canSchedulePodOnNode( entry.getValue(), task.getPod(), entry.getKey() ) ){
                result.add( entry.getKey() );
            }
        }
        return result;
    }

    LinkedList<Task> getUpcomingTasksCopy() {
        return new LinkedList<>( upcomingTasks );
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
                    if ( pwa.getSpec().getNodeName() == null ) {
                        scheduler.schedulePod( pwa );
                    }
                    break;
                case MODIFIED:
                    if (!pod.getStatus().getContainerStatuses().isEmpty() && pod.getStatus().getContainerStatuses().get(0).getState().getTerminated() != null) {
                        scheduler.onPodTermination(pwa);
                    } else {
                        final Task task = scheduler.getTaskByPod(pwa);
                        task.setPod( pwa );
                    }
                    break;
                case DELETED:
                    scheduler.markPodAsDeleted(pwa);
                    break;
                default: log.info( "No implementation for {}", action );
            }

        }


        @Override
        public void onClose(WatcherException cause) {
            log.info( "Watcher was closed" );
        }

    }


}
