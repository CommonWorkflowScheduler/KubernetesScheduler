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
import io.fabric8.kubernetes.api.model.ContainerStatus;
import io.fabric8.kubernetes.api.model.Node;
import io.fabric8.kubernetes.api.model.Pod;
import io.fabric8.kubernetes.client.Watcher;
import lombok.Getter;
import lombok.extern.slf4j.Slf4j;

import java.io.File;
import java.io.IOException;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.stream.Collectors;

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
    void assignTaskToNode( NodeTaskAlignment alignment ) {
        final NodeTaskFilesAlignment nodeTaskFilesAlignment = (NodeTaskFilesAlignment) alignment;
        final List< TaskInputFileLocationWrapper > locationWrappers = writeInitConfig( nodeTaskFilesAlignment );
        alignment.task.setCopiedFiles( locationWrappers );
        getCopyStrategy().generateCopyScript( alignment.task );
        final List<LocationWrapper> allLocationWrappers = nodeTaskFilesAlignment.fileAlignment.getAllLocationWrappers();
        alignment.task.setInputFiles( allLocationWrappers );
        useLocations( allLocationWrappers );
        super.assignTaskToNode( alignment );
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

    List<TaskInputFileLocationWrapper> writeInitConfig( NodeTaskFilesAlignment alignment ) {

        final File config = new File(alignment.task.getWorkingDir() + '/' + ".command.inputs.json");
        LinkedList< TaskInputFileLocationWrapper > inputFiles = new LinkedList<>();
        try {
            final Inputs inputs = new Inputs(
                    this.getDns() + "/daemon/" + getNamespace() + "/" + getExecution() + "/"
            );
            for (Map.Entry<String, List<FilePath>> entry : alignment.fileAlignment.nodeFileAlignment.entrySet()) {
                if( entry.getKey().equals( alignment.node.getMetadata().getName() ) ) continue;
                final List<String> collect = entry  .getValue()
                                                    .stream()
                                                    .map(x -> x.path)
                                                    .collect(Collectors.toList());
                inputs.data.add( new InputEntry( getDaemonOnNode( entry.getKey() ), entry.getKey(), collect ) );

                final NodeLocation location = NodeLocation.getLocation( entry.getKey() );
                for (FilePath filePath : entry.getValue()) {
                    final LocationWrapper locationWrapper = filePath.file.getLocationWrapper(location);
                    inputFiles.add(
                            new TaskInputFileLocationWrapper(
                                filePath.file,
                                locationWrapper.getCopyOf( location )
                            )
                    );
                }

            }
            inputs.symlinks.addAll(alignment.fileAlignment.symlinks);

            new ObjectMapper().writeValue( config, inputs );
            return inputFiles;
        } catch (IOException e) {
            e.printStackTrace();
        }
        return null;
    }

    TaskInputs getInputsOfTask(Task task ){
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

    private void podWasInitialized( Pod pod ){
        final Task task = changeStateOfTask(pod, State.PREPARED);
        task.getCopiedFiles().parallelStream().forEach( TaskInputFileLocationWrapper::apply );
    }

    @Override
    boolean canSchedulePodOnNode( PodRequirements availableByNode, PodWithAge pod, NodeWithAlloc node ) {
        return this.getDaemonOnNode( node ) != null && super.canSchedulePodOnNode( availableByNode, pod, node );
    }

    /**
     * Remove all Nodes with a location contained in taskInputs.excludedNodes
     * @param matchingNodes
     * @param taskInputs
     */
    void filterMatchingNodesForTask( Set<NodeWithAlloc> matchingNodes, TaskInputs taskInputs ){
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
