package cws.k8s.scheduler.scheduler;

import cws.k8s.scheduler.client.CWSKubernetesClient;
import cws.k8s.scheduler.model.*;
import cws.k8s.scheduler.model.location.LocationType;
import cws.k8s.scheduler.model.location.NodeLocation;
import cws.k8s.scheduler.model.location.hierachy.*;
import cws.k8s.scheduler.model.outfiles.OutputFile;
import cws.k8s.scheduler.model.outfiles.PathLocationWrapperPair;
import cws.k8s.scheduler.model.outfiles.SymlinkOutput;
import cws.k8s.scheduler.model.taskinputs.SymlinkInput;
import cws.k8s.scheduler.model.taskinputs.TaskInputs;
import cws.k8s.scheduler.publishDir.PublishItem;
import cws.k8s.scheduler.publishDir.PublishManager;
import cws.k8s.scheduler.rest.exceptions.NotARealFileException;
import cws.k8s.scheduler.rest.response.getfile.FileResponse;
import cws.k8s.scheduler.util.DaemonHolder;
import cws.k8s.scheduler.util.copying.CurrentlyCopying;
import cws.k8s.scheduler.util.copying.CurrentlyCopyingOnNode;
import io.fabric8.kubernetes.api.model.Node;
import io.fabric8.kubernetes.api.model.Pod;
import io.fabric8.kubernetes.client.Watcher;
import lombok.AccessLevel;
import lombok.Getter;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.net.ftp.FTPClient;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;

@Slf4j
public abstract class SchedulerWithDaemonSet extends Scheduler {

    @Getter(AccessLevel.PROTECTED)
    final DaemonHolder daemonHolder = new DaemonHolder();
    @Getter
    private String workflowEngineNode = null;
    final HierarchyWrapper hierarchyWrapper;
    private final InputFileCollector inputFileCollector;
    private final ConcurrentHashMap<Long, LocationWrapper> requestedLocations = new ConcurrentHashMap<>();
    final String localWorkDir;
    private final PublishManager publishManager;

    /**
     * Which node is currently copying files from which node
     */
    @Getter(AccessLevel.PACKAGE)
    private final CurrentlyCopying currentlyCopying = new CurrentlyCopying();

    SchedulerWithDaemonSet( String execution, CWSKubernetesClient client, String namespace, SchedulerConfig config) {
        super(execution, client, namespace, config);
        this.hierarchyWrapper = new HierarchyWrapper( config.localWorkDir );
        this.inputFileCollector = new InputFileCollector( hierarchyWrapper );
        if ( config.copyStrategy == null ) {
            throw new IllegalArgumentException( "Copy strategy is null" );
        }
        this.localWorkDir = config.localWorkDir;
        publishManager = new PublishManager( hierarchyWrapper, Path.of( config.workDir ), Path.of(this.localWorkDir) );
    }

    public String getDaemonIpOnNode( String node ){
        return daemonHolder.getDaemonIp( node );
    }

    public String getDaemonNameOnNode( String node ){
        return daemonHolder.getDaemonName( node );
    }

    String getDaemonIpOnNode( Node node ){
        return getDaemonIpOnNode( node.getMetadata().getName() );
    }

    /**
     * Mark all locationWrappers as used
     */
    void useLocations( List<LocationWrapper> locationWrappers ){
        locationWrappers.parallelStream().forEach( LocationWrapper::use );
    }

    /**
     * Mark all locationWrappers as unused
     */
    void freeLocations( List<LocationWrapper> locationWrappers ){
        locationWrappers.parallelStream().forEach( LocationWrapper::free );
    }

    @Override
    void undoTaskScheduling( Task task ){
        if ( task.getInputFiles() != null ) {
            freeLocations( task.getInputFiles() );
            task.setInputFiles( null );
        }
        if ( task.getCopyingToNode() != null ) {
            removeFromCopyingToNode( task, task.getNode().getNodeLocation(), task.getCopyingToNode());
            task.setCopyingToNode( null );
        }
        task.setCopiedFiles( null );
        task.setNode( null );
    }

    @Override
    int terminateTasks(List<Task> finishedTasks) {
        final TaskResultParser taskResultParser = new TaskResultParser();
        finishedTasks.parallelStream().forEach( finishedTask -> {
            try{
                freeLocations( finishedTask.getInputFiles() );
                if ( !"DeadlineExceeded".equals( finishedTask.getPod().getStatus().getReason() ) ) { //If Deadline exceeded, task cannot write out files and containerStatuses.terminated is not available
                    final Integer exitCode = finishedTask.getPod().getStatus().getContainerStatuses().get(0).getState().getTerminated().getExitCode();
                    log.info( "Pod finished with exitCode: {}", exitCode );
                    //Init failure
                    final Path workdir = Paths.get(finishedTask.getWorkingDir());
                    if ( exitCode == 123 && Files.exists( workdir.resolve(".command.init.failure") ) ) {
                        log.info( "Task {} ({}) had an init failure: won't parse the in- and out files", finishedTask.getConfig().getRunName(), finishedTask.getConfig().getName() );
                    } else {
                        final Set<OutputFile> newAndUpdatedFiles = taskResultParser.getNewAndUpdatedFiles(
                                workdir,
                                finishedTask.getNode().getNodeLocation(),
                                !finishedTask.wasSuccessfullyExecuted(),
                                finishedTask
                        );
                        for (OutputFile newAndUpdatedFile : newAndUpdatedFiles) {
                            if( newAndUpdatedFile instanceof PathLocationWrapperPair pathLocationWrapperPair ) {
                                hierarchyWrapper.addFile(
                                        newAndUpdatedFile.getPath(),
                                        pathLocationWrapperPair.getLocationWrapper()
                                );
                            } else if ( newAndUpdatedFile instanceof SymlinkOutput symlinkOutput ){
                                hierarchyWrapper.addSymlink( newAndUpdatedFile.getPath(), symlinkOutput.getDst() );
                            }
                        }
                    }
                }
            } catch ( Exception e ){
                log.info( "Problem while finishing task: {} ({})", finishedTask.getConfig().getRunName(), finishedTask.getConfig().getName(), e );
            }
            super.taskWasFinished( finishedTask );
        });
        return 0;
    }

    /**
     * Register that file is copied to node
     */
    void addToCopyingToNode( Task task, NodeLocation nodeLocation, CurrentlyCopyingOnNode toAdd ){
        if ( nodeLocation == null ) {
            throw new IllegalArgumentException( "NodeLocation cannot be null" );
        }
        currentlyCopying.add( task, nodeLocation, toAdd );
    }

    /**
     * Remove that file is copied to node
     */
    void removeFromCopyingToNode( Task task, NodeLocation nodeLocation, CurrentlyCopyingOnNode toRemove ) {
        if (nodeLocation == null) {
            throw new IllegalArgumentException("NodeLocation cannot be null");
        }
        currentlyCopying.remove( task, nodeLocation, toRemove );
    }

    TaskInputs getInputsOfTask(Task task ) throws NoAlignmentFoundException {
        return inputFileCollector.getInputsOfTask( task, client.getNumberOfNodes() );
    }


    public FileResponse nodeOfLastFileVersion( String path ) throws NotARealFileException {
        LinkedList<SymlinkInput> symlinks = new LinkedList<>();
        Path currentPath = Paths.get(path);
        HierarchyFile currentFile = hierarchyWrapper.getFile( currentPath );
        while ( currentFile instanceof LinkHierarchyFile linkFile ){
            symlinks.add( new SymlinkInput( currentPath, linkFile.getDst() ) );
            currentPath = linkFile.getDst();
            currentFile = hierarchyWrapper.getFile( currentPath );
        }
        Collections.reverse( symlinks );
        //File is maybe out of scope
        if ( currentFile == null ) {
            return new FileResponse( currentPath.toString(), symlinks );
        }
        if ( ! (currentFile instanceof RealHierarchyFile file) ){
            log.info( "File was: {}", currentFile );
            throw new NotARealFileException();
        }
        final LocationWrapper lastUpdate = file.getLastUpdate(LocationType.NODE);
        if( lastUpdate == null ) {
            return null;
        }
        requestedLocations.put( lastUpdate.getId(), lastUpdate );
        String node = lastUpdate.getLocation().getIdentifier();
        return new FileResponse( currentPath.toString(), node, getDaemonIpOnNode(node), node.equals(workflowEngineNode), symlinks, lastUpdate.getId() );
    }

    /**
     * Register a new local file
     */
    public void addFile( String path, long size, long timestamp, long locationWrapperID, boolean overwrite, String node ){
        final NodeLocation location = NodeLocation.getLocation( node == null ? workflowEngineNode : node );

        LocationWrapper locationWrapper;
        if( !overwrite && locationWrapperID != -1 ){
            locationWrapper = requestedLocations.get( locationWrapperID ).getCopyOf( location );
        } else {
            locationWrapper = new LocationWrapper( location, timestamp, size );
        }

        hierarchyWrapper.addFile( Paths.get( path ), overwrite, locationWrapper );
    }

    public void addPublishItem( PublishItem item ) {
        publishManager.addPublishItem( item );
    }

    public int getUnpublishedItems() {
        return publishManager.getUnpublishedCount();
    }

    FTPClient getConnection( String daemon ){
        int trial = 0;
        while ( true ) {
            try {
                FTPClient f = new FTPClient();
                f.connect(daemon);
                f.login("ftp", "nextflowClient");
                f.enterLocalPassiveMode();
                return f;
            } catch ( IOException e ) {
                if ( trial > 5 ) {
                    throw new RuntimeException(e);
                }
                log.error("Cannot create FTP client: {}", daemon);
                try {
                    Thread.sleep((long) Math.pow(2, trial++));
                } catch (InterruptedException ex) {
                    Thread.currentThread().interrupt();
                    log.error( "Interrupted while waiting for retry to connect to FTP client", e);
                }
            }
        }
    }

    public void taskHasFinishedCopyTask( String name ){
        final Task task = tasksByPodName.get( name );
        task.getNode().startingTaskCopyingDataFinished( task );
        informResourceChange();
    }

    /**
     * Since task was not yet initialized: set scheduled
     * @param task task that was scheduled
     */
    @Override
    void taskWasScheduledSetState( Task task ){
        task.getState().setState( State.SCHEDULED );
    }

    public void setWorkflowEngineNode( String ip ){
        this.workflowEngineNode = client.getPodByIp( ip ).getSpec().getNodeName();
        log.info( "WorkflowEngineNode was set to {}", workflowEngineNode );
    }

    @Override
    void podEventReceived(Watcher.Action action, Pod pod){
        //noinspection LoopConditionNotUpdatedInsideLoop
        while ( daemonHolder == null ){
            //The Watcher can be started before the class is initialized
            try {
                Thread.sleep(20);
            } catch (InterruptedException ignored) {
                Thread.currentThread().interrupt();
            }
        }
        if( ( "mount-" + this.getExecution().replace('_', '-') + "-" ).equals(pod.getMetadata().getGenerateName()) ){
            final String nodeName = pod.getSpec().getNodeName();
            if ( nodeName != null ){
                synchronized ( daemonHolder ) {
                    final String podName = pod.getMetadata().getName();
                    final boolean podIsCurrentDaemon = pod.getStatus().getPodIP() != null && pod.getStatus().getPodIP().equals(daemonHolder.getDaemonIp(nodeName));
                    if ( action == Watcher.Action.DELETED ) {
                        if (podIsCurrentDaemon) {
                            daemonHolder.removeDaemon(nodeName);
                        }
                    } else if ( pod.getStatus().getPhase().equals("Running") ) {
                        daemonHolder.addDaemon( nodeName, podName, pod.getStatus().getPodIP() );
                        informResourceChange();
                    } else if ( podIsCurrentDaemon ) {
                        daemonHolder.removeDaemon(nodeName);
                        if( !pod.getStatus().getPhase().equals("Failed") ){
                            log.info( "Unexpected phase {} for daemon: {}", pod.getStatus().getPhase(), podName );
                        }
                    }
                }
            }
        }
    }

    @Override
    public void close() {
        log.info( "There are {} item(s) not copied!", publishManager.getUnpublishedCount() );
        super.close();
    }
}
