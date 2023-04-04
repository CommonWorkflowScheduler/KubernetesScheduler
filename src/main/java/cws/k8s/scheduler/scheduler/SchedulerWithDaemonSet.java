package cws.k8s.scheduler.scheduler;

import com.fasterxml.jackson.databind.ObjectMapper;
import cws.k8s.scheduler.model.*;
import cws.k8s.scheduler.model.location.hierachy.*;
import cws.k8s.scheduler.scheduler.schedulingstrategy.InputEntry;
import cws.k8s.scheduler.scheduler.schedulingstrategy.Inputs;
import cws.k8s.scheduler.client.KubernetesClient;
import cws.k8s.scheduler.util.*;
import cws.k8s.scheduler.model.location.Location;
import cws.k8s.scheduler.model.location.LocationType;
import cws.k8s.scheduler.model.location.NodeLocation;
import cws.k8s.scheduler.model.outfiles.OutputFile;
import cws.k8s.scheduler.model.outfiles.PathLocationWrapperPair;
import cws.k8s.scheduler.model.outfiles.SymlinkOutput;
import cws.k8s.scheduler.model.taskinputs.SymlinkInput;
import cws.k8s.scheduler.model.taskinputs.TaskInputs;
import cws.k8s.scheduler.rest.exceptions.NotARealFileException;
import cws.k8s.scheduler.rest.response.getfile.FileResponse;
import cws.k8s.scheduler.scheduler.copystrategy.CopyStrategy;
import cws.k8s.scheduler.scheduler.copystrategy.FTPstrategy;
import cws.k8s.scheduler.scheduler.outlabel.OutLabelHolder;
import cws.k8s.scheduler.scheduler.outlabel.HolderMaxTasks;
import cws.k8s.scheduler.util.copying.CurrentlyCopying;
import cws.k8s.scheduler.util.copying.CurrentlyCopyingOnNode;
import io.fabric8.kubernetes.api.model.*;
import io.fabric8.kubernetes.client.Watcher;
import lombok.AccessLevel;
import lombok.Getter;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.net.ftp.FTPClient;

import java.io.*;
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
    @Getter
    private final CopyStrategy copyStrategy;
    final HierarchyWrapper hierarchyWrapper;
    private final InputFileCollector inputFileCollector;
    private final ConcurrentHashMap<Long, LocationWrapper> requestedLocations = new ConcurrentHashMap<>();
    final String localWorkDir;
    protected final OutLabelHolder outLabelHolder = new HolderMaxTasks() ;

    /**
     * Which node is currently copying files from which node
     */
    @Getter(AccessLevel.PACKAGE)
    private final CurrentlyCopying currentlyCopying = new CurrentlyCopying();

    SchedulerWithDaemonSet(String execution, KubernetesClient client, String namespace, SchedulerConfig config) {
        super(execution, client, namespace, config);
        this.hierarchyWrapper = new HierarchyWrapper( config.workDir );
        this.inputFileCollector = new InputFileCollector( hierarchyWrapper );
        if ( config.copyStrategy == null ) {
            throw new IllegalArgumentException( "Copy strategy is null" );
        }
        switch ( config.copyStrategy ){
            case "ftp":
            case "copy":
                copyStrategy = new FTPstrategy();
                break;
            default:
                throw new IllegalArgumentException( "Copy strategy is unknown " + config.copyStrategy );
        }
        this.localWorkDir = config.workDir;
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
    void assignPodToNode( PodWithAge pod, NodeTaskAlignment alignment ) {
        if ( !pod.getSpec().getInitContainers().isEmpty() && ((NodeTaskFilesAlignment) alignment).isRemoveInit() ) {
            log.info( "Removing init container from pod {}", pod.getMetadata().getName() );
            client.assignPodToNodeAndRemoveInit( pod, alignment.node.getName() );
        } else {
            super.assignPodToNode( pod, alignment );
        }
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
                        log.info( "Task " + finishedTask.getConfig().getRunName() + " (" + finishedTask.getConfig().getName() + ") had an init failure: won't parse the in- and out files" );
                    } else {
                        final Set<OutputFile> newAndUpdatedFiles = taskResultParser.getNewAndUpdatedFiles(
                                workdir,
                                finishedTask.getNode().getNodeLocation(),
                                !finishedTask.wasSuccessfullyExecuted(),
                                finishedTask
                        );
                        for (OutputFile newAndUpdatedFile : newAndUpdatedFiles) {
                            if( newAndUpdatedFile instanceof PathLocationWrapperPair ) {
                                hierarchyWrapper.addFile(
                                        newAndUpdatedFile.getPath(),
                                        ((PathLocationWrapperPair) newAndUpdatedFile).getLocationWrapper()
                                );
                            } else if ( newAndUpdatedFile instanceof SymlinkOutput ){
                                hierarchyWrapper.addSymlink(
                                        newAndUpdatedFile.getPath(),
                                        ((SymlinkOutput) newAndUpdatedFile).getDst()
                                );
                            }
                        }
                    }
                }
            } catch ( Exception e ){
                log.info( "Problem while finishing task: " + finishedTask.getConfig().getRunName() + " (" + finishedTask.getConfig().getName() + ")", e );
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

    /**
     *
     * @return null if the task cannot be scheduled
     */
    WriteConfigResult writeInitConfig( NodeTaskFilesAlignment alignment ) {

        final File config = new File(alignment.task.getWorkingDir() + '/' + ".command.inputs.json");
        LinkedList<TaskInputFileLocationWrapper> inputFiles = new LinkedList<>();
        Map<String, Task> waitForTask = new HashMap<>();
        final CurrentlyCopyingOnNode filesForCurrentNode = new CurrentlyCopyingOnNode();
        final NodeLocation currentNode = alignment.node.getNodeLocation();

        try {
            final Inputs inputs = new Inputs(
                    this.getDns(),
                    getExecution(),
                    this.localWorkDir + "/sync/",
                    alignment.task.getConfig().getRunName(),
                    100
            );

            for (Map.Entry<Location, AlignmentWrapper> entry : alignment.fileAlignment.getNodeFileAlignment().entrySet()) {
                if( entry.getKey() == currentNode ) {
                    continue;
                }

                final NodeLocation location = (NodeLocation) entry.getKey();
                final AlignmentWrapper alignmentWrapper = entry.getValue();
                for ( FilePathWithTask filePath : alignmentWrapper.getWaitFor()) {
                    //Node copies currently from somewhere else!
                    //May be problematic if the task depending on fails/is stopped before all files are downloaded
                    waitForTask.put( filePath.getPath(), filePath.getTask() );
                }

                final List<String> collect = new LinkedList<>();
                for (FilePath filePath : alignmentWrapper.getFilesToCopy()) {
                    final LocationWrapper locationWrapper = filePath.getFile().getLocationWrapper(location);
                    inputFiles.add(
                            new TaskInputFileLocationWrapper(
                                    filePath.getPath(),
                                    filePath.getFile(),
                                    locationWrapper.getCopyOf( currentNode )
                            )
                    );
                    collect.add(filePath.getPath());
                    filesForCurrentNode.add( filePath.getPath(), alignment.task, location );
                }
                if( !collect.isEmpty() ) {
                    inputs.data.add(new InputEntry( getDaemonIpOnNode(entry.getKey().getIdentifier()), entry.getKey().getIdentifier(), collect, alignmentWrapper.getToCopySize()));
                }
            }

            boolean copyDataToNode = !inputs.data.isEmpty();
            inputs.waitForTask( waitForTask );
            inputs.symlinks.addAll(alignment.fileAlignment.getSymlinks());
            inputs.sortData();
            final boolean allEmpty = !copyDataToNode && inputs.symlinks.isEmpty() && inputs.waitForFilesOfTask.isEmpty();
            if ( !allEmpty ) {
                new ObjectMapper().writeValue( config, inputs );
            }
            return new WriteConfigResult( inputFiles, waitForTask, filesForCurrentNode, !allEmpty, copyDataToNode);

        } catch (IOException e) {
            log.error( "Cannot write " + config, e);
        }
        
        return null;
    }

    TaskInputs getInputsOfTask(Task task ) throws NoAlignmentFoundException {
        return inputFileCollector.getInputsOfTask( task, client.getNumberOfNodes() );
    }


    public FileResponse nodeOfLastFileVersion( String path ) throws NotARealFileException {
        LinkedList<SymlinkInput> symlinks = new LinkedList<>();
        Path currentPath = Paths.get(path);
        HierarchyFile currentFile = hierarchyWrapper.getFile( currentPath );
        while ( currentFile instanceof LinkHierarchyFile){
            final LinkHierarchyFile linkFile = (LinkHierarchyFile) currentFile;
            symlinks.add( new SymlinkInput( currentPath, linkFile.getDst() ) );
            currentPath = linkFile.getDst();
            currentFile = hierarchyWrapper.getFile( currentPath );
        }
        Collections.reverse( symlinks );
        //File is maybe out of scope
        if ( currentFile == null ) {
            return new FileResponse( currentPath.toString(), symlinks );
        }
        if ( ! (currentFile instanceof RealHierarchyFile) ){
            log.info( "File was: {}", currentFile );
            throw new NotARealFileException();
        }
        final RealHierarchyFile file = (RealHierarchyFile) currentFile;
        final LocationWrapper lastUpdate = file.getLastUpdate(LocationType.NODE);
        if( lastUpdate == null ) {
            return null;
        }
        requestedLocations.put( lastUpdate.getId(), lastUpdate );
        String node = lastUpdate.getLocation().getIdentifier();
        return new FileResponse( currentPath.toString(), node, getDaemonIpOnNode(node), node.equals(workflowEngineNode), symlinks, lastUpdate.getId() );
    }

    MatchingFilesAndNodes getMatchingFilesAndNodes( final Task task, final Map<NodeWithAlloc, Requirements> availableByNode ){
        final Set<NodeWithAlloc> matchingNodesForTask = getMatchingNodesForTask(availableByNode, task);
        if( matchingNodesForTask.isEmpty() ) {
            log.trace( "No node with enough resources for {}", task.getConfig().getRunName() );
            return null;
        }

        final TaskInputs inputsOfTask;
        try {
            inputsOfTask = getInputsOfTask(task);
        } catch (NoAlignmentFoundException e) {
            return null;
        }
        if( inputsOfTask == null ) {
            log.info( "No node where the pod can start, pod: {}", task.getConfig().getRunName() );
            return null;
        }

        filterNotMatchingNodesForTask( matchingNodesForTask, inputsOfTask );
        if( matchingNodesForTask.isEmpty() ) {
            log.info( "No node which fulfills all requirements {}", task.getConfig().getRunName() );
            return null;
        }

        return new MatchingFilesAndNodes( matchingNodesForTask, inputsOfTask );
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

    private void handleProblematicInit( Task task ){
        String file = this.localWorkDir + "/sync/" + task.getConfig().getRunName();
        try {
            Map<String,TaskInputFileLocationWrapper> wrapperByPath = new HashMap<>();
            task.getCopiedFiles().forEach( x -> wrapperByPath.put( x.getPath(), x ));
            log.info( "Get daemon on node {}; daemons: {}", task.getNode().getNodeLocation().getIdentifier(), daemonHolder );
            final InputStream inputStream = getConnection( getDaemonIpOnNode(task.getNode().getNodeLocation().getIdentifier())).retrieveFileStream(file);
            if (inputStream == null) {
                //Init has not even started
                return;
            }
            Scanner scanner = new Scanner(inputStream);
            Set<String> openedFiles = new HashSet<>();
            while( scanner.hasNext() ){
                String line = scanner.nextLine();
                if ( line.startsWith( "S-" ) ){
                    openedFiles.add( line.substring( 2 ) );
                } else if ( line.startsWith( "F-" ) ){
                    openedFiles.remove( line.substring( 2 ) );
                    wrapperByPath.get( line.substring( 2 ) ).success();
                    log.info("task {}, file: {} success", task.getConfig().getName(), line);
                }
            }
            for ( String openedFile : openedFiles ) {
                wrapperByPath.get( openedFile ).failure();
                log.info("task {}, file: {} deactivated on node {}", task.getConfig().getName(), openedFile, wrapperByPath.get( openedFile ).getWrapper().getLocation());
            }
        } catch ( Exception e ){
            log.error( "Can't handle failed init from pod " + task.getPod().getName(), e);
        }
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

    private void podWasInitialized( Pod pod ){
        final Integer exitCode = pod.getStatus().getInitContainerStatuses().get(0).getState().getTerminated().getExitCode();
        final Task task = changeStateOfTask(pod, exitCode == 0 ? State.PREPARED : State.INIT_WITH_ERRORS);
        task.setPod( new PodWithAge( pod ) );
        log.info( "Pod {}, Init Code: {}", pod.getMetadata().getName(), exitCode);
        removeFromCopyingToNode( task, task.getNode().getNodeLocation(), task.getCopyingToNode() );
        if( exitCode == 0 ){
            task.getCopiedFiles().parallelStream().forEach( TaskInputFileLocationWrapper::success );
        } else {
            handleProblematicInit( task );
            task.setInputFiles( null );
        }
        task.setCopiedFiles( null );
        task.setCopyingToNode( null );
    }

    /**
     * Remove all Nodes with a location contained in taskInputs.excludedNodes
     */
    void filterNotMatchingNodesForTask(Set<NodeWithAlloc> matchingNodes, TaskInputs taskInputs ){
        final Iterator<NodeWithAlloc> iterator = matchingNodes.iterator();
        final Set<Location> excludedNodes = taskInputs.getExcludedNodes();
        while ( iterator.hasNext() ){
            final NodeWithAlloc next = iterator.next();
            if( excludedNodes.contains( next.getNodeLocation() ) ){
                iterator.remove();
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

    @Override
    void podEventReceived(Watcher.Action action, Pod pod){
        if ( pod.getMetadata().getName().equals( this.getExecution().replace('_', '-') ) ){
            this.workflowEngineNode = pod.getSpec().getNodeName();
            log.info( "WorkflowEngineNode was set to {}", workflowEngineNode );
        }
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
        } else if ( this.getName().equals(pod.getSpec().getSchedulerName())
                && action == Watcher.Action.MODIFIED
                && getTaskByPod( pod ).getState().getState() == State.SCHEDULED )
        {
            final List<ContainerStatus> initContainerStatuses = pod.getStatus().getInitContainerStatuses();
            if ( ! initContainerStatuses.isEmpty() && initContainerStatuses.get(0).getState().getTerminated() != null ) {
                podWasInitialized( pod );
            }
        }
    }

}
