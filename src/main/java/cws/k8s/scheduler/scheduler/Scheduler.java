package cws.k8s.scheduler.scheduler;

import cws.k8s.scheduler.model.*;
import cws.k8s.scheduler.client.Informable;
import cws.k8s.scheduler.client.KubernetesClient;
import cws.k8s.scheduler.dag.DAG;
import cws.k8s.scheduler.util.Batch;
import cws.k8s.scheduler.util.NodeTaskAlignment;
import io.fabric8.kubernetes.api.model.Pod;
import io.fabric8.kubernetes.client.Watcher;
import io.fabric8.kubernetes.client.informers.ResourceEventHandler;
import io.fabric8.kubernetes.client.informers.SharedIndexInformer;
import lombok.Getter;
import lombok.extern.slf4j.Slf4j;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.util.*;

@Slf4j
public abstract class Scheduler implements Informable {

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
    private final List<Task> unscheduledTasks = new ArrayList<>( 100 );
    private final List<Task> unfinishedTasks = new ArrayList<>( 100 );
    final Map<String, Task> tasksByPodName = new HashMap<>();
    final Map<Integer, Task> tasksById = new HashMap<>();
    private final SharedIndexInformer<Pod> podHandler;
    private final TaskprocessingThread schedulingThread;
    private final TaskprocessingThread finishThread;

    final boolean traceEnabled;

    Scheduler(String execution, KubernetesClient client, String namespace, SchedulerConfig config){
        this.execution = execution;
        this.name = System.getenv( "SCHEDULER_NAME" ) + "-" + execution;
        this.namespace = namespace;
        log.trace( "Register scheduler for " + this.name );
        this.client = client;
        this.dns = config.dns.endsWith( "/" ) ? config.dns : config.dns + "/";
        this.dag = new DAG();
        this.traceEnabled = config.traceEnabled;

        PodHandler handler = new PodHandler(this );

        schedulingThread = new TaskprocessingThread( unscheduledTasks, this::schedule );
        schedulingThread.start();

        finishThread = new TaskprocessingThread(unfinishedTasks, this::terminateTasks );
        finishThread.start();

        log.info("Start watching: {}", this.namespace );

        this.podHandler = client.pods().inNamespace( this.namespace ).inform( handler );
        this.podHandler.start();

        log.info("Watching");
    }

    /* Abstract methods */

    /**
     * @return the number of unscheduled Tasks
     */
    public int schedule( final List<Task> unscheduledTasks ) {
        final LinkedList<Task> unscheduledTasksCopy = new LinkedList<>( unscheduledTasks );
        long startSchedule = System.currentTimeMillis();
        if( traceEnabled ) {
            unscheduledTasks.forEach( x -> x.getTraceRecord().tryToSchedule( startSchedule ) );
        }
        final ScheduleObject scheduleObject = getTaskNodeAlignment(unscheduledTasks, getAvailableByNode( true ));
        final List<NodeTaskAlignment> taskNodeAlignment = scheduleObject.getTaskAlignments();

        //check if still possible...
        if ( scheduleObject.isCheckStillPossible() ) {
            boolean possible = validSchedulePlan ( taskNodeAlignment );
            if (!possible) {
                log.info("The whole scheduling plan is not possible anymore.");
                informOtherResourceChange();
                return taskNodeAlignment.size();
            }
        }

        int failure = 0;
        int scheduled = 0;
        for (NodeTaskAlignment nodeTaskAlignment : taskNodeAlignment) {
            try {
                if (isClose()) {
                    return -1;
                }
                if ( !assignTaskToNode( nodeTaskAlignment ) ){
                    if ( scheduleObject.isStopSubmitIfOneFails() ) {
                        return taskNodeAlignment.size() - scheduled;
                    }
                    failure++;
                    continue;
                }
            } catch ( Exception e ){
                log.info( "Could not schedule task: {} undo all", nodeTaskAlignment.task.getConfig().getRunName() );
                e.printStackTrace();
                undoTaskScheduling( nodeTaskAlignment.task );
                if ( scheduleObject.isStopSubmitIfOneFails() ) {
                    return taskNodeAlignment.size() - scheduled;
                }
                continue;
            }
            taskWasScheduled(nodeTaskAlignment.task);
            unscheduledTasksCopy.remove( nodeTaskAlignment.task );
            scheduled++;
        }
        //Use instance object that does not contain yet scheduled tasks
        postScheduling( unscheduledTasksCopy, getAvailableByNode( false ) );
        return unscheduledTasks.size() - taskNodeAlignment.size() + failure;
    }

    /**
     * This method is called when a SchedulePlan was successfully executed.
     * @param unscheduledTasks
     */
    void postScheduling( final List<Task> unscheduledTasks, Map<NodeWithAlloc, Requirements> availableByNode ) {}

    /**
     * Call this method in case of any scheduling problems
     */
    void undoTaskScheduling( Task task ){}


    public boolean validSchedulePlan( List<NodeTaskAlignment> taskNodeAlignment ){
        Map< NodeWithAlloc, Requirements> availableByNode = getAvailableByNode( false );
        for ( NodeTaskAlignment nodeTaskAlignment : taskNodeAlignment ) {
            final Requirements requirements = availableByNode.get(nodeTaskAlignment.node);
            if ( requirements == null ) {
                log.info( "Node {} is not available anymore", nodeTaskAlignment.node.getMetadata().getName() );
                return false;
            }
            requirements.subFromThis(nodeTaskAlignment.task.getPod().getRequest());
        }
        for ( Map.Entry<NodeWithAlloc, Requirements> e : availableByNode.entrySet() ) {
            if ( ! e.getValue().higherOrEquals( ImmutableRequirements.ZERO ) ) {
                log.info( "Node {} has not enough resources. Available: {}, After assignment it would be: {}", e.getKey().getMetadata().getName(), e.getKey().getAvailableResources(), e.getValue() );
                return false;
            }
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
        if( t == null ) {
            return;
        }
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
        if ( task == null ) {
            return;
        }
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
            batch.informSchedulable( task );
            synchronized (batchHelper) {
                tryToScheduleBatch( batch );
            }
        }
    }

    /**
     * Synchronize calls via batchHelper
     */
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

    Task createTask( TaskConfig conf ){
        return new Task( conf, dag );
    }

    /* External access to Tasks */
    public void addTask( int id, TaskConfig conf ) {
        final Task task = createTask( conf );
        synchronized ( tasksByPodName ) {
            if ( !tasksByPodName.containsKey( conf.getRunName() ) ) {
                tasksByPodName.put( conf.getRunName(), task );
            }
        }
        synchronized ( tasksById ) {
            if ( !tasksById.containsKey( id ) ) {
                tasksById.put( id, task );
            }
        }
        synchronized ( upcomingTasks ) {
            upcomingTasks.add( task );
        }
        if( currentBatchInstance != null ){
            currentBatchInstance.registerTask( task );
        }
    }

    public boolean removeTask( int id ) {
        final Task task;
        synchronized ( tasksById ) {
            task = tasksById.get( id );
            }
        if ( task == null ) {
            return false;
        }
        synchronized ( tasksByPodName ) {
            tasksByPodName.remove( task.getConfig().getRunName() );
        }
        synchronized ( upcomingTasks ) {
            upcomingTasks.remove( task );
        }
        return true;
    }

    /**
     * Chooses best param for a task
     */
    public Map<String, Object> getSchedulerParams( String taskname, String name ){
        return new HashMap<>();
    }

    public TaskState getTaskState( int id ) {
        synchronized ( tasksById ) {
            if ( tasksById.containsKey( id ) ) {
                return tasksById.get( id ).getState();
            }
        }
        return null;
    }

    /* Nodes */

    /**
     * Checks if a node fulfills all requirements for a pod. This means: <br>
     * - enough resources available <br>
     * - Affinities match
     */
    public boolean canSchedulePodOnNode(Requirements availableByNode, PodWithAge pod, NodeWithAlloc node ) {
        if ( availableByNode == null ) {
            return false;
        }
        return canSchedulePodOnNode( pod, node ) && availableByNode.higherOrEquals( pod.getRequest() );
    }

    public boolean canSchedulePodOnNode(PodWithAge pod, NodeWithAlloc node ) {
        return node.canScheduleNewPod() && affinitiesMatch( pod, node );
    }

    boolean affinitiesMatch( PodWithAge pod, NodeWithAlloc node ){

        final boolean nodeCouldRunThisPod = node.getMaxResources().higherOrEquals( pod.getRequest() );
        if ( !nodeCouldRunThisPod ){
            return false;
        }

        final Map<String, String> podsNodeSelector = pod.getSpec().getNodeSelector();
        final Map<String, String> nodesLabels = node.getMetadata().getLabels();
        if ( podsNodeSelector == null || podsNodeSelector.isEmpty() ) {
            return true;
        }
        //cannot be fulfilled if podsNodeSelector is not empty
        if ( nodesLabels == null || nodesLabels.isEmpty() ) {
            return false;
        }

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
     */
    boolean canPodBeScheduled( PodWithAge pod, NodeWithAlloc node ){
        return node.canSchedule( pod );
    }

    void assignPodToNode( PodWithAge pod, NodeTaskAlignment alignment ){
        client.assignPodToNode( pod, alignment.node.getMetadata().getName() );
    }

    boolean assignTaskToNode( NodeTaskAlignment alignment ){

        final File nodeFile = new File(alignment.task.getWorkingDir() + '/' + ".command.node");

        try(BufferedWriter printWriter = new BufferedWriter( new FileWriter( nodeFile ))){
            printWriter.write( alignment.node.getName() );
            printWriter.write( '\n' );
        } catch (IOException e) {
            log.error( "Cannot write " + nodeFile, e);
        }

        alignment.task.setNode( alignment.node );

        final PodWithAge pod = alignment.task.getPod();

        alignment.node.addPod( pod, alignment.task.isCopiesDataToNode() );

        log.info ( "Assign pod: " + pod.getMetadata().getName() + " to node: " + alignment.node.getMetadata().getName() );

        assignPodToNode( pod, alignment );

        pod.getSpec().setNodeName( alignment.node.getMetadata().getName() );
        log.info ( "Assigned pod to:" + pod.getSpec().getNodeName());

        alignment.task.submitted();
        if( traceEnabled ) {
            alignment.task.getTraceRecord().submitted();
            alignment.task.writeTrace();
        }

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
     * @return returns the task, if the state was changed
     */
    Task changeStateOfTask(Pod pod, State state) {
        Task t = getTaskByPod(pod);
        if (t != null) {
            synchronized (t.getState()) {
                if (t.getState().getState().level < state.level) {
                    t.getState().setState(state);
                    return t;
                } else {
                    log.debug("Task {} was already in state {} and cannot be changed to {}", t.getConfig().getRunName(),
                            t.getState().getState(), state);
                    return null;
                }
            }
        }
        return null;
    }

    /**
     * starts the scheduling routine
     */
    public void informResourceChange() {
        informOtherResourceChange();
        synchronized (unscheduledTasks){
            unscheduledTasks.notifyAll();
        }
    }

    /**
     * Call if something was changed withing the scheduling loop. In all other cases, call informResourceChange()
     */
    private void informOtherResourceChange() {
        schedulingThread.otherResourceChange();
    }

    Task getTaskByPod( Pod pod ) {
        Task t = null;
        synchronized ( tasksByPodName ) {
            if ( tasksByPodName.containsKey( pod.getMetadata().getName() ) ) {
                t = tasksByPodName.get( pod.getMetadata().getName() );
            }
        }

        if ( t == null ){
            throw new IllegalStateException( "No task with config found for: " + pod.getMetadata().getName() );
        }

        return t;
    }

    Map<NodeWithAlloc, Requirements> getAvailableByNode( boolean logging ){
        Map<NodeWithAlloc, Requirements> availableByNode = new HashMap<>();
        final List<String> logInfo;
        if ( logging ){
            logInfo = new LinkedList<>();
            logInfo.add("------------------------------------");
        } else {
            logInfo = null;
        }
        for (NodeWithAlloc item : getNodeList()) {
            if ( !item.isReady() ) {
                continue;
            }
            final Requirements availableResources = item.getAvailableResources();
            availableByNode.put(item, availableResources);
            if ( logging ){
                logInfo.add("Node: " + item.getName() + " " + availableResources);
            }
        }
        if ( logging ) {
            logInfo.add("------------------------------------");
            log.info(String.join("\n", logInfo));
        }
        return availableByNode;
    }

    /**
     * Filters all nodes, that have enough resources and fulfill the affinities
     */
    public Set<NodeWithAlloc> getMatchingNodesForTask( Map<NodeWithAlloc, Requirements> availableByNode, Task task ){
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
        podHandler.close();
        schedulingThread.interrupt();
        finishThread.interrupt();
        this.close = true;
    }

    static class PodHandler implements ResourceEventHandler<Pod> {

        private final Scheduler scheduler;

        public PodHandler( Scheduler scheduler ) {
            this.scheduler = scheduler;
        }

        public void eventReceived( Watcher.Action action, Pod pod) {

            scheduler.podEventReceived(action, pod);

            if (!scheduler.name.equals(pod.getSpec().getSchedulerName())) {
                return;
            }

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
                    if ( "DeadlineExceeded".equals( pod.getStatus().getReason() ) || //Task ran out of time
                            ( !pod.getStatus().getContainerStatuses().isEmpty() &&
                                    pod.getStatus().getContainerStatuses().get(0).getState().getTerminated() != null )
                    ) {
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
