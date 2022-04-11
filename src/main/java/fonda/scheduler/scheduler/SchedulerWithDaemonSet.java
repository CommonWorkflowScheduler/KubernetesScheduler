package fonda.scheduler.scheduler;

import com.fasterxml.jackson.databind.ObjectMapper;
import fonda.scheduler.client.KubernetesClient;
import fonda.scheduler.model.*;
import fonda.scheduler.model.location.Location;
import fonda.scheduler.model.location.LocationType;
import fonda.scheduler.model.location.NodeLocation;
import fonda.scheduler.model.location.hierachy.*;
import fonda.scheduler.model.outfiles.OutputFile;
import fonda.scheduler.model.outfiles.PathLocationWrapperPair;
import fonda.scheduler.model.outfiles.SymlinkOutput;
import fonda.scheduler.model.taskinputs.SymlinkInput;
import fonda.scheduler.model.taskinputs.TaskInputs;
import fonda.scheduler.rest.exceptions.NotARealFileException;
import fonda.scheduler.rest.response.getfile.FileResponse;
import fonda.scheduler.scheduler.copystrategy.CopyStrategy;
import fonda.scheduler.scheduler.copystrategy.FTPstrategy;
import fonda.scheduler.scheduler.schedulingstrategy.InputEntry;
import fonda.scheduler.scheduler.schedulingstrategy.Inputs;
import fonda.scheduler.util.FilePath;
import fonda.scheduler.util.NodeTaskAlignment;
import fonda.scheduler.util.NodeTaskFilesAlignment;
import fonda.scheduler.util.Tuple;
import io.fabric8.kubernetes.api.model.ContainerStatus;
import io.fabric8.kubernetes.api.model.Node;
import io.fabric8.kubernetes.api.model.Pod;
import io.fabric8.kubernetes.client.Watcher;
import lombok.Getter;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.net.ftp.FTPClient;

import java.io.*;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;

@Slf4j
public abstract class SchedulerWithDaemonSet extends Scheduler {

    private final Map<String, String> daemonByNode = new HashMap<>();
    @Getter
    private String workflowEngineNode = null;
    @Getter
    private final CopyStrategy copyStrategy;
    final HierarchyWrapper hierarchyWrapper;
    private final InputFileCollector inputFileCollector;
    private final ConcurrentHashMap<Long,LocationWrapper> requestedLocations = new ConcurrentHashMap<>();
    private final String localWorkDir;

    SchedulerWithDaemonSet(String execution, KubernetesClient client, String namespace, SchedulerConfig config) {
        super(execution, client, namespace, config);
        this.hierarchyWrapper = new HierarchyWrapper( config.workDir );
        this.inputFileCollector = new InputFileCollector( hierarchyWrapper );
        if ( config.copyStrategy == null ) throw new IllegalArgumentException( "Copy strategy is null" );
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

    public String getDaemonOnNode( String node ){
        synchronized ( daemonByNode ) {
            return daemonByNode.get( node );
        }
    }

    String getDaemonOnNode( Node node ){
        return getDaemonOnNode( node.getMetadata().getName() );
    }

    private void useLocations( List<LocationWrapper> locationWrappers ){
        locationWrappers.parallelStream().forEach( LocationWrapper::use );
    }

    private void freeLocations( List<LocationWrapper> locationWrappers ){
        locationWrappers.parallelStream().forEach( LocationWrapper::free );
    }
    
    @Override
    boolean assignTaskToNode( NodeTaskAlignment alignment ) {
        final NodeTaskFilesAlignment nodeTaskFilesAlignment = (NodeTaskFilesAlignment) alignment;
        final WriteConfigResult writeConfigResult = writeInitConfig(nodeTaskFilesAlignment);
        if ( writeConfigResult == null ) return false;
        alignment.task.setCopiedFiles( writeConfigResult.getInputFiles() );
        addToCopyingToNode( alignment.node.getNodeLocation(), writeConfigResult.getCopyingToNode() );
        alignment.task.setCopyingToNode( writeConfigResult.getCopyingToNode() );
        getCopyStrategy().generateCopyScript( alignment.task );
        final List<LocationWrapper> allLocationWrappers = nodeTaskFilesAlignment.fileAlignment.getAllLocationWrappers();
        alignment.task.setInputFiles( allLocationWrappers );
        useLocations( allLocationWrappers );
        return super.assignTaskToNode( alignment );
    }

    void undoTaskScheduling( Task task ){
        if ( task.getInputFiles() != null ) {
            freeLocations( task.getInputFiles() );
            task.setInputFiles( null );
        }
        if ( task.getCopyingToNode() != null ) {
            removeFromCopyingToNode( task.getNode(), task.getCopyingToNode());
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
                final Set<OutputFile> newAndUpdatedFiles = taskResultParser.getNewAndUpdatedFiles(
                        Paths.get(finishedTask.getWorkingDir()),
                        finishedTask.getNode(),
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
            } catch ( Exception e ){
                log.info( "Problem while finishing task: " + finishedTask.getConfig().getHash() + " (" + finishedTask.getConfig().getName() + ")", e );
            }
            super.taskWasFinished( finishedTask );
        });
        return 0;
    }

    private final Map< NodeLocation, HashMap< String, Tuple<Task,Location>> > copyingToNode = new HashMap<>();

    private void addToCopyingToNode(  NodeLocation nodeLocation, HashMap< String, Tuple<Task,Location> > toAdd ){
        if ( nodeLocation == null ) throw new IllegalArgumentException( "NodeLocation cannot be null" );
        if ( copyingToNode.containsKey( nodeLocation ) ){
            final HashMap<String, Tuple<Task, Location>> stringTupleHashMap = copyingToNode.get( nodeLocation );
            stringTupleHashMap.putAll( toAdd );
        } else {
            copyingToNode.put( nodeLocation, toAdd );
        }
    }

    private void removeFromCopyingToNode(NodeLocation nodeLocation, HashMap< String, Tuple<Task,Location>> toRemove ){
        if ( nodeLocation == null ) throw new IllegalArgumentException( "NodeLocation cannot be null" );
        copyingToNode.get( nodeLocation ).keySet().removeAll( toRemove.keySet() );
    }

    WriteConfigResult writeInitConfig( NodeTaskFilesAlignment alignment ) {

        final File config = new File(alignment.task.getWorkingDir() + '/' + ".command.inputs.json");

        LinkedList< TaskInputFileLocationWrapper > inputFiles = new LinkedList<>();
        Map<String, Task> waitForTask = new HashMap<>();

        try {
            final Inputs inputs = new Inputs(
                    this.getDns() + "/daemon/" + getNamespace() + "/" + getExecution() + "/",
                    this.localWorkDir + "/sync/",
                    alignment.task.getConfig().getHash()
            );


            final HashMap<String, Tuple<Task, Location>> filesForCurrentNode = new HashMap<>();
            final NodeLocation currentNode = alignment.node.getNodeLocation();
            final HashMap<String, Tuple<Task, Location>> filesOnCurrentNode = copyingToNode.get(currentNode);

            for (Map.Entry<String, List<FilePath>> entry : alignment.fileAlignment.nodeFileAlignment.entrySet()) {
                if( entry.getKey().equals( alignment.node.getMetadata().getName() ) ) continue;

                final List<String> collect = new LinkedList<>();

                final NodeLocation location = NodeLocation.getLocation( entry.getKey() );
                for (FilePath filePath : entry.getValue()) {
                    if ( filesOnCurrentNode != null && filesOnCurrentNode.containsKey( filePath.path ) ) {
                        //Node copies currently from somewhere else!
                        final Tuple<Task, Location> taskLocationTuple = filesOnCurrentNode.get(filePath.path);
                        if ( taskLocationTuple.getB() != location ) return null;
                        else {
                            //May be problematic if the task depending on fails/is stopped before all files are downloaded
                            waitForTask.put( filePath.path, taskLocationTuple.getA() );
                        }
                    } else {
                        final LocationWrapper locationWrapper = filePath.file.getLocationWrapper(location);
                        inputFiles.add(
                                new TaskInputFileLocationWrapper(
                                        filePath.path,
                                        filePath.file,
                                        locationWrapper.getCopyOf( currentNode )
                                )
                        );
                        collect.add(filePath.path );
                        filesForCurrentNode.put( filePath.path, new Tuple<>( alignment.task, location  ) );
                    }
                }
                if( !collect.isEmpty() ) {
                    inputs.data.add(new InputEntry(getDaemonOnNode(entry.getKey()), entry.getKey(), collect));
                }
            }
            inputs.waitForTask( waitForTask );
            inputs.symlinks.addAll(alignment.fileAlignment.symlinks);

            new ObjectMapper().writeValue( config, inputs );
            return new WriteConfigResult( inputFiles, waitForTask, filesForCurrentNode );
        } catch (IOException e) {
            e.printStackTrace();
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
        if( lastUpdate == null ) return null;
        requestedLocations.put( lastUpdate.getId(), lastUpdate );
        String node = lastUpdate.getLocation().getIdentifier();
        return new FileResponse( currentPath.toString(), node, getDaemonOnNode(node), node.equals(workflowEngineNode), symlinks, lastUpdate.getId() );
    }

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
        String file = this.localWorkDir + "/sync/" + task.getConfig().getHash();
        try {
            Map<String,TaskInputFileLocationWrapper> wrapperByPath = new HashMap<>();
            task.getCopiedFiles().stream().forEach( x -> wrapperByPath.put( x.getPath(), x ));
            log.info( "Get daemon on node {}; daemons: {}", task.getNode().getIdentifier(), daemonByNode );
            final InputStream inputStream = getConnection(getDaemonOnNode(task.getNode().getIdentifier())).retrieveFileStream(file);
            if (inputStream == null) {
                //Init has not even started
                return;
            }
            Scanner scanner = new Scanner(inputStream);
            Set<String> openedFiles = new HashSet();
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
            log.info( "Can't handle failed init from pod " + task.getPod().getName());
            e.printStackTrace();
        }
    }

    private FTPClient getConnection( String daemon ){
        int trial = 0;
        while ( true ) {
            try {
                FTPClient f = new FTPClient();
                f.connect(daemon);
                f.login("ftp", "nextflowClient");
                f.enterLocalPassiveMode();
                return f;
            } catch ( IOException e ) {
                if ( trial > 5 ) throw new RuntimeException(e);
                log.error("Cannot create FTP client: {}", daemon);
                try {
                    Thread.sleep((long) Math.pow(2, trial++));
                } catch (InterruptedException ex) {
                    ex.printStackTrace();
                }
            }
        }
    }

    private void podWasInitialized( Pod pod ){
        final Integer exitCode = pod.getStatus().getInitContainerStatuses().get(0).getState().getTerminated().getExitCode();
        final Task task = changeStateOfTask(pod, exitCode == 0 ? State.PREPARED : State.INIT_WITH_ERRORS);
        task.setPod( new PodWithAge( pod ) );
        log.info( "Pod {}, Init Code: {}", pod.getMetadata().getName(), exitCode);
        removeFromCopyingToNode( task.getNode(), task.getCopyingToNode() );
        if( exitCode == 0 ){
            task.getCopiedFiles().parallelStream().forEach( TaskInputFileLocationWrapper::success );
        } else {
            handleProblematicInit( task );
            task.setInputFiles( null );
        }
        task.setCopiedFiles( null );
        task.setCopyingToNode( null );
    }

    @Override
    boolean canSchedulePodOnNode(Requirements availableByNode, PodWithAge pod, NodeWithAlloc node ) {
        return this.getDaemonOnNode( node ) != null && super.canSchedulePodOnNode( availableByNode, pod, node );
    }

    /**
     * Remove all Nodes with a location contained in taskInputs.excludedNodes
     * @param matchingNodes
     * @param taskInputs
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
        while ( daemonByNode == null ){
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
                synchronized ( daemonByNode ) {
                    final String podName = pod.getMetadata().getName();
                    final boolean podIsCurrentDaemon = podName.equals(daemonByNode.get(nodeName));
                    if ( action == Watcher.Action.DELETED ) {
                        if (podIsCurrentDaemon) {
                            daemonByNode.remove(nodeName);
                        }
                    } else if ( pod.getStatus().getPhase().equals("Running") ) {
                        daemonByNode.put( nodeName, pod.getStatus().getPodIP() );
                        informResourceChange();
                    } else if ( podIsCurrentDaemon ) {
                        daemonByNode.remove(nodeName);
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
