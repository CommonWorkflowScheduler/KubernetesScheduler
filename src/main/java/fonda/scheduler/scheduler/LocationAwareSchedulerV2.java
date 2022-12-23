package fonda.scheduler.scheduler;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import fonda.scheduler.client.KubernetesClient;
import fonda.scheduler.model.*;
import fonda.scheduler.model.location.Location;
import fonda.scheduler.model.location.NodeLocation;
import fonda.scheduler.model.location.hierachy.LocationWrapper;
import fonda.scheduler.model.location.hierachy.NoAlignmentFoundException;
import fonda.scheduler.model.taskinputs.SymlinkInput;
import fonda.scheduler.model.taskinputs.TaskInputs;
import fonda.scheduler.scheduler.data.TaskInputsNodes;
import fonda.scheduler.scheduler.filealignment.InputAlignment;
import fonda.scheduler.scheduler.filealignment.costfunctions.NoAligmentPossibleException;
import fonda.scheduler.scheduler.schedulingstrategy.InputEntry;
import fonda.scheduler.scheduler.schedulingstrategy.Inputs;
import fonda.scheduler.util.*;
import fonda.scheduler.util.copying.CurrentlyCopying;
import fonda.scheduler.util.copying.CurrentlyCopyingOnNode;
import io.fabric8.kubernetes.api.model.Status;
import io.fabric8.kubernetes.client.dsl.ExecListener;
import io.fabric8.kubernetes.client.dsl.ExecWatch;
import lombok.*;
import lombok.extern.slf4j.Slf4j;

import java.io.*;
import java.util.*;
import java.util.stream.Collectors;

@Slf4j
public abstract class LocationAwareSchedulerV2 extends SchedulerWithDaemonSet {

    @Getter(AccessLevel.PACKAGE)
    private final InputAlignment inputAlignment;
    @Getter(AccessLevel.PACKAGE)
    private final int maxCopyTasksPerNode;
    @Getter(AccessLevel.PACKAGE)
    private final int maxWaitingCopyTasksPerNode;

    @Getter(AccessLevel.PACKAGE)
    private final CopyToNodeManager copyToNodeManager = new CopyToNodeManager();

    private final LogCopyTask logCopyTask = new LogCopyTask();

    /**
     * This lock is used to syncronize the creation of the copy tasks and the finishing.
     * Otherwise, it could happen that:
     * 1. Copy tasks checks for data already on the node
     * 2. Copy tasks finished, data is removed from currently copying and added to onNode
     * 3. Task checks if data is currently copied -> no -> create a new task
     */
    private final Object copyLock = new Object();

    public LocationAwareSchedulerV2(
            String name,
            KubernetesClient client,
            String namespace,
            SchedulerConfig config,
            InputAlignment inputAlignment) {
        super( name, client, namespace, config );
        this.inputAlignment = inputAlignment;
        this.maxCopyTasksPerNode = config.maxCopyTasksPerNode == null ? 1 : config.maxCopyTasksPerNode;
        this.maxWaitingCopyTasksPerNode = config.maxWaitingCopyTasksPerNode == null ? 1 : config.maxWaitingCopyTasksPerNode;
    }

    protected abstract List<NodeTaskLocalFilesAlignment> createAlignmentForTasksWithAllDataOnNode(
            List<TaskInputsNodes> taskWithAllData,
            Map<NodeWithAlloc, Requirements> availableByNode
    );

    @Override
    public ScheduleObject getTaskNodeAlignment(
            final List<Task> unscheduledTasks,
            final Map<NodeWithAlloc, Requirements> availableByNode
    ){
        final List<TaskInputsNodes> tasksAndData = unscheduledTasks
                .parallelStream()
                .map( task -> {
                    final TaskInputs inputsOfTask = extractInputsOfData( task );
                    if ( inputsOfTask == null ) return null;
                    //all nodes that contain all files
                    final List<NodeWithAlloc> nodesWithAllData = availableByNode
                            .keySet()
                            .stream()
                            .filter( node -> {
                                final CurrentlyCopyingOnNode copyingFilesToNode = getCurrentlyCopying().get( node.getNodeLocation() );
                                //File version does not match and is in use
                                return !inputsOfTask.getExcludedNodes().contains( node.getNodeLocation() )
                                        //Affinities are correct and the node can run new pods
                                        && canSchedulePodOnNode( task.getPod(), node )
                                        //All files are on the node and no copy task is overwriting them
                                        && inputsOfTask.allFilesAreOnLocationAndNotOverwritten( node.getNodeLocation(), copyingFilesToNode.getAllFilesCurrentlyCopying() );
                            } )
                            .collect( Collectors.toList() );
                    return new TaskInputsNodes( task, nodesWithAllData, inputsOfTask );
                } )
                .filter( Objects::nonNull )
                .collect( Collectors.toList() );

        final List<TaskInputsNodes> taskWithAllData = tasksAndData
                .stream()
                .filter( td -> !td.getNodesWithAllData().isEmpty() )
                .collect( Collectors.toList() );
        final List<NodeTaskLocalFilesAlignment> alignment = createAlignmentForTasksWithAllDataOnNode(taskWithAllData, availableByNode);
        final ScheduleObject scheduleObject = new ScheduleObject( (List) alignment );
        scheduleObject.setCheckStillPossible( true );
        scheduleObject.setStopSubmitIfOneFails( true );
        return scheduleObject;
    }

    @Override
    void postScheduling( List<Task> unscheduledTasks ) {
        final List<NodeWithAlloc> allNodes = client.getAllNodes();
        final List<NodeTaskFilesAlignment> nodeTaskFilesAlignments;
        synchronized ( copyLock ) {
            final List<DataOnNode> tasksAndData = unscheduledTasks
                    .parallelStream()
                    .map( task -> {
                        final TaskInputs inputsOfTask = extractInputsOfData( task );
                        if ( inputsOfTask == null ) return null;
                        final DataOnNode dataOnNode = getDataOnNode( task, inputsOfTask, allNodes );
                        return dataOnNode;
                    } )
                    .filter( Objects::nonNull )
                    .collect( Collectors.toList() );
            nodeTaskFilesAlignments = createCopyTasks( tasksAndData );
        }
        nodeTaskFilesAlignments.parallelStream().forEach( this::startCopyTasks );
    }
    
    private void startCopyTasks( NodeTaskFilesAlignment nodeTaskFilesAlignment ) {
        final CopyTask copyTask = initializeCopyTask( nodeTaskFilesAlignment );
        String[] command = new String[3];
        command[0] = "/bin/bash";
        command[1] = "-c";
        command[2] = "cd " + nodeTaskFilesAlignment.task.getWorkingDir() + " && ";
        final String json;
        try {
            json = new ObjectMapper()
                    .writeValueAsString( copyTask.getInputs() )
                    .replace( "\"", "\\\"" );
        } catch ( JsonProcessingException e ) {
            throw new RuntimeException( e );
        }
        command[2] += "/code/ftp.py false " + nodeTaskFilesAlignment.node.getName() + " \"" + json + "\"";
        String name = nodeTaskFilesAlignment.task.getConfig().getName() + "-copy-" + nodeTaskFilesAlignment.node.getName();
        log.info( "Starting {} to node {}", nodeTaskFilesAlignment.task.getConfig().getName(), nodeTaskFilesAlignment.node.getName() );
        String files = "";
        for (TaskInputFileLocationWrapper t : copyTask.getInputFiles()) {
            files += t.getPath() + ", ";
        }
        logCopyTask.copy( nodeTaskFilesAlignment.task.getConfig().getName(), nodeTaskFilesAlignment.node.getName(), copyTask.getInputFiles().size(), "start (" +  files + ")" );
        client.execCommand( getDaemonNameOnNode( copyTask.getNodeLocation().getIdentifier() ), getNamespace(), command, new LaListener( copyTask, name, nodeTaskFilesAlignment ) );
    }

    @RequiredArgsConstructor
    private class LaListener implements MyExecListner {

        @Setter
        private ExecWatch exec;
        private final CopyTask copyTask;
        private final String name;
        @Setter
        private ByteArrayOutputStream out = new ByteArrayOutputStream();
        @Setter
        private ByteArrayOutputStream error = new ByteArrayOutputStream();
        private boolean finished = false;

        private final NodeTaskFilesAlignment nodeTaskFilesAlignment;

        @Override
        public void onClose( int exitCode, String reason ) {
            if ( !finished ) {
                log.error( "Copy task was not finished, but closed. ExitCode: " + exitCode + " Reason: " + reason );
                copyTaskFinished( copyTask, exitCode == 0 );
            }
            informResourceChange();
        }

        @Override
        public void onFailure( Throwable t, Response failureResponse ) {
            log.info( name + " failed, output: ", t );
            log.info( name + " Exec Output: {} ", out );
            log.info( name + " Exec Error Output: {} ", error );
            exec.close();
            logCopyTask.copy( nodeTaskFilesAlignment.task.getConfig().getName(), nodeTaskFilesAlignment.node.getName(), copyTask.getInputFiles().size(), "failed" );
        }

        @Override
        public void onExit( int exitCode, Status reason ) {
            finished = true;
            log.info( name + " was finished exitCode = {}, reason = {}", exitCode, reason );
            log.debug( name + " Exec Output: {} ", out );
            log.debug( name + " Exec Error Output: {} ", error );
            copyTaskFinished( copyTask, exitCode == 0 );
            exec.close();
            logCopyTask.copy( nodeTaskFilesAlignment.task.getConfig().getName(), nodeTaskFilesAlignment.node.getName(), copyTask.getInputFiles().size(), "finished(" + exitCode + ")" );
        }
    }

    /**
     * Creates config and reserves files
     *
     * @param nodeTaskFilesAlignment
     * @return
     */
    CopyTask initializeCopyTask( NodeTaskFilesAlignment nodeTaskFilesAlignment ) {
        final CopyTask copyTask = createCopyTask(nodeTaskFilesAlignment);
        copyTask.setNodeLocation( nodeTaskFilesAlignment.node.getNodeLocation() );
        final List<LocationWrapper> allLocationWrappers = nodeTaskFilesAlignment.fileAlignment.getAllLocationWrappers();
        copyTask.setAllLocationWrapper( allLocationWrappers );

        log.info( "addToCopyingToNode task: {}, node: {}, data: {}, currentlyCopying: {}", nodeTaskFilesAlignment.task.getConfig().getName(), nodeTaskFilesAlignment.node.getName(), copyTask.getFilesForCurrentNode(), getCurrentlyCopying().get( nodeTaskFilesAlignment.node.getNodeLocation() ) );
        //Files that will be copied
        addToCopyingToNode( nodeTaskFilesAlignment.node.getNodeLocation(), copyTask.getFilesForCurrentNode() );
        useLocations( allLocationWrappers );
        return copyTask;
    }

    private void copyTaskFinished( CopyTask copyTask, boolean success ) {
        if( success ){
            synchronized ( copyLock ) {
                copyTask.getInputFiles().parallelStream().forEach( TaskInputFileLocationWrapper::success );
                removeFromCopyingToNode( copyTask.getNodeLocation(), copyTask.getFilesForCurrentNode() );
            }
            freeLocations( copyTask.getAllLocationWrapper() );
        } else {
            synchronized ( copyLock ) {
                removeFromCopyingToNode( copyTask.getNodeLocation(), copyTask.getFilesForCurrentNode() );
                handleProblematicInit( copyTask );
            }
        }
    }

    private void handleProblematicInit( CopyTask copyTask ){
        String file = this.localWorkDir + "/sync/" + copyTask.getInputs().execution;
        try {
            Map<String,TaskInputFileLocationWrapper> wrapperByPath = new HashMap<>();
            copyTask.getInputFiles().forEach( x -> wrapperByPath.put( x.getPath(), x ));
            log.info( "Get daemon on node {}; daemons: {}", copyTask.getNodeLocation().getIdentifier(), daemonHolder );
            final InputStream inputStream = getConnection( getDaemonIpOnNode(copyTask.getNodeLocation().getIdentifier())).retrieveFileStream(file);
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
                    log.info("task {}, file: {} success", copyTask.getInputs().execution, line);
                }
            }
            for ( String openedFile : openedFiles ) {
                wrapperByPath.get( openedFile ).failure();
                log.info("task {}, file: {} deactivated on node {}", copyTask.getInputs().execution, openedFile, wrapperByPath.get( openedFile ).getWrapper().getLocation());
            }
        } catch ( Exception e ){
            log.error( "Can't handle failed init from pod " + copyTask.getInputs().execution, e);
        }
    }


    /**
     * @param alignment
     * @return null if the task cannot be scheduled
     */
    CopyTask createCopyTask( NodeTaskFilesAlignment alignment ) {
        LinkedList< TaskInputFileLocationWrapper > inputFiles = new LinkedList<>();
        final CurrentlyCopyingOnNode filesForCurrentNode = new CurrentlyCopyingOnNode();
        final NodeLocation currentNode = alignment.node.getNodeLocation();
        final Inputs inputs = new Inputs(
                this.getDns(),
                getExecution(),
                this.localWorkDir + "/sync/",
                alignment.task.getConfig().getRunName()
        );

        for (Map.Entry<Location, AlignmentWrapper> entry : alignment.fileAlignment.getNodeFileAlignment().entrySet()) {
            if( entry.getKey() == currentNode ) {
                continue;
            }

            final NodeLocation location = (NodeLocation) entry.getKey();
            final AlignmentWrapper alignmentWrapper = entry.getValue();

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

        inputs.sortData();
        return new CopyTask( inputs, inputFiles, filesForCurrentNode);
    }

    List<NodeTaskFilesAlignment> createCopyTasks( List<DataOnNode> tasksAndData ){
        final CurrentlyCopying planedToCopy = new CurrentlyCopying();
        tasksAndData.sort( Comparator.comparing( DataOnNode::getNodesWithAllData ).reversed() );
        final Map<NodeWithAlloc, Integer> assignedPodsByNode = copyToNodeManager.getCurrentlyCopyingTasksOnNode();
        List<NodeTaskFilesAlignment> nodeTaskAlignments = new LinkedList<>();
        for ( DataOnNode dataOnNode : tasksAndData ) {
            final Tuple<NodeWithAlloc, FileAlignment> result = calculateBestNode( dataOnNode, planedToCopy );
            if ( result != null && assignedPodsByNode.getOrDefault( result.getA(), 0 ) < getMaxCopyTasksPerNode() ) {
                addAlignmentToPlanned( planedToCopy, result.getB().getNodeFileAlignment(), dataOnNode.getTask(), result.getA() );
                nodeTaskAlignments.add( new NodeTaskFilesAlignment( result.getA(), dataOnNode.getTask(), result.getB() ) );
                assignedPodsByNode.put( result.getA(), assignedPodsByNode.getOrDefault( result.getA(), 0 ) + 1 );
            }
        }
        return nodeTaskAlignments;
    }

    Tuple<NodeWithAlloc, FileAlignment> calculateBestNode( final DataOnNode taskData, CurrentlyCopying planedToCopy ) {
        FileAlignment bestAlignment = null;
        NodeWithAlloc bestNode = null;
        final Set<NodeWithAlloc> matchingNodes = taskData.getNodes();
        int triedNodes = 0;
        int noAlignmentFound = 0;
        int couldStopFetching = 0;
        final List<Double> costs = traceEnabled ? new LinkedList<>() : null;
        for ( final NodeWithAlloc node : matchingNodes) {

            final CurrentlyCopyingOnNode currentlyCopying = getCurrentlyCopying().get(node.getNodeLocation());
            final CurrentlyCopyingOnNode currentlyPlanedToCopy = planedToCopy.get(node.getNodeLocation());
            FileAlignment fileAlignment = null;
            try {
                fileAlignment = inputAlignment.getInputAlignment(
                        taskData.getTask(),
                        taskData.getInputsOfTask(),
                        node,
                        currentlyCopying,
                        currentlyPlanedToCopy,
                        bestAlignment == null ? Double.MAX_VALUE : bestAlignment.getCost()
                );

                if ( fileAlignment == null ){
                    couldStopFetching++;
                } else {
                    log.info( "Task: {}, node: {}, bestWeight: {}, currentWeight: {}",
                            taskData.getTask().getConfig().getName(),
                            node.getNodeLocation(),
                            bestAlignment == null ? null : bestAlignment.getWorth(),
                            fileAlignment.getWorth()
                    );

                    if (
                            //Anything to copy?
                            fileAlignment.copyFromSomewhere( node.getNodeLocation() ) &&
                            //Not set or better than current best
                                    ( bestAlignment == null || bestAlignment.getWorth() > fileAlignment.getWorth() ) ) {
                        bestAlignment = fileAlignment;
                        bestNode = node;
                        log.info( "Best alignment for task: {} costs: {}", taskData.getTask().getConfig().getRunName(), fileAlignment.getCost() );
                    }
                }
            } catch ( NoAligmentPossibleException e ){
                noAlignmentFound++;
                log.info( "Task: {} - {}", taskData.getTask().getConfig().getName() , e.getMessage() );
            }
            if ( traceEnabled ) {
                triedNodes++;
                final Double thisRoundCost = fileAlignment == null
                        ? null
                        : fileAlignment.getCost();
                costs.add( thisRoundCost );
            }
        }

        if ( bestAlignment == null ) {
            return null;
        }

        logCopyTask.log( taskData.getTask().getConfig().getName() + " " + bestAlignment );

        return new Tuple<>( bestNode, bestAlignment );
    }


    private TaskInputs extractInputsOfData( Task task ) {
        if ( task == null ) return null;
        final TaskInputs inputsOfTask;
        try {
            inputsOfTask = getInputsOfTask( task );
        } catch ( NoAlignmentFoundException e ) {
            return null;
        }
        if ( inputsOfTask == null ) {
            log.info( "No node where the pod can start, pod: {}", task.getConfig().getRunName() );
            return null;
        }
        return inputsOfTask;
    }


    private DataOnNode getDataOnNode( Task task, TaskInputs inputsOfTask, List<NodeWithAlloc> allNodes ) {
        final DataOnNode dataOnNode = new DataOnNode( task, inputsOfTask );
        for ( NodeWithAlloc node : allNodes ) {
            final long avgSize = inputsOfTask.calculateAvgSize();
            if ( !inputsOfTask.getExcludedNodes().contains( node ) && affinitiesMatch( task.getPod(), node ) ) {
                final Tuple<Boolean, Long> booleanLongTuple = inputsOfTask.calculateDataOnNodeAdditionalInfo( node.getNodeLocation() );
                if ( booleanLongTuple.getA() ) {
                    dataOnNode.allData( node );
                } else if ( booleanLongTuple.getB() >= avgSize ) {
                    //As we only calculate the average size of all files, it can happen that more than 100% are on the node
                    dataOnNode.addData( node, 0.999 );
                } else {
                    dataOnNode.addData( node, booleanLongTuple.getB() / avgSize );
                }
            }
        }
        return dataOnNode;
    }

    @Override
    boolean assignTaskToNode( NodeTaskAlignment alignment ) {
        final NodeTaskLocalFilesAlignment nodeTaskFilesAlignment = (NodeTaskLocalFilesAlignment) alignment;

        if ( !nodeTaskFilesAlignment.symlinks.isEmpty() ) {
            try ( BufferedWriter writer = new BufferedWriter( new FileWriter( alignment.task.getWorkingDir() + '/' + ".command.symlinks" ) ) ) {
                writer.write( "create_symlink() {" );
                writer.newLine();
                writer.write( "  rm -rf \"$2\"" );
                writer.newLine();
                writer.write( "  mkdir -p \"$(dirname \"$2\")\"" );
                writer.newLine();
                writer.write( "  ln -s \"$1\" \"$2\"" );
                writer.newLine();
                writer.write( "}" );
                writer.newLine();
                for ( SymlinkInput symlink : nodeTaskFilesAlignment.symlinks ) {
                    writer.write( "create_symlink \"" + symlink.getDst().replace( "\"", "\\\"" ) + "\" \"" + symlink.getSrc().replace( "\"", "\\\"" ) + "\"" );
                    writer.newLine();
                }
            } catch ( IOException ex ) {
                ex.printStackTrace();
            }
        }

        alignment.task.setInputFiles( nodeTaskFilesAlignment.locationWrappers );
        useLocations( nodeTaskFilesAlignment.locationWrappers );

        return super.assignTaskToNode( alignment );
    }

    @Override
    public void close() {
        logCopyTask.close();
        super.close();
    }
}
