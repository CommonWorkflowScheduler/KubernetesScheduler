package fonda.scheduler.scheduler;

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
import fonda.scheduler.scheduler.la2.MinCopyingComparator;
import fonda.scheduler.scheduler.la2.MinSizeComparator;
import fonda.scheduler.scheduler.la2.TaskStat;
import fonda.scheduler.scheduler.la2.TaskStatComparator;
import fonda.scheduler.scheduler.la2.capacityavailable.SimpleCapacityAvailableToNode;
import fonda.scheduler.scheduler.la2.copystrategy.CopyRunner;
import fonda.scheduler.scheduler.la2.copystrategy.ShellCopy;
import fonda.scheduler.scheduler.la2.ready2run.ReadyToRunToNode;
import fonda.scheduler.scheduler.schedulingstrategy.InputEntry;
import fonda.scheduler.scheduler.schedulingstrategy.Inputs;
import fonda.scheduler.util.*;
import fonda.scheduler.util.copying.CurrentlyCopying;
import fonda.scheduler.util.copying.CurrentlyCopyingOnNode;
import fonda.scheduler.util.score.FileSizeRankScore;
import lombok.*;
import lombok.extern.slf4j.Slf4j;

import java.io.*;
import java.util.*;
import java.util.stream.Collectors;

@Slf4j
public class LocationAwareSchedulerV2 extends SchedulerWithDaemonSet {

    @Getter(AccessLevel.PACKAGE)
    private final InputAlignment inputAlignment;
    @Getter(AccessLevel.PACKAGE)
    private final int maxCopyTasksPerNode;
    @Getter(AccessLevel.PACKAGE)
    private final int maxWaitingCopyTasksPerNode;

    private final LogCopyTask logCopyTask = new LogCopyTask();

    private final ReadyToRunToNode readyToRunToNode;

    private final CopyRunner copyRunner;

    private final SimpleCapacityAvailableToNode capacityAvailableToNode;

    private final TaskStatComparator phaseTwoComparator;

    private final int copySameTaskInParallel;

    /**
     * This lock is used to synchronize the creation of the copy tasks and the finishing.
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
            InputAlignment inputAlignment,
            ReadyToRunToNode readyToRunToNode ) {
        super( name, client, namespace, config );
        this.inputAlignment = inputAlignment;
        this.maxCopyTasksPerNode = config.maxCopyTasksPerNode == null ? 1 : config.maxCopyTasksPerNode;
        this.maxWaitingCopyTasksPerNode = config.maxWaitingCopyTasksPerNode == null ? 1 : config.maxWaitingCopyTasksPerNode;
        this.readyToRunToNode = readyToRunToNode;
        this.readyToRunToNode.init( new FileSizeRankScore( hierarchyWrapper ) );
        readyToRunToNode.setLogger( logCopyTask );
        this.copyRunner = new ShellCopy( client, this, logCopyTask );
        this.copySameTaskInParallel = 2;
        capacityAvailableToNode = new SimpleCapacityAvailableToNode( getCurrentlyCopying(), inputAlignment, this.copySameTaskInParallel );
        phaseTwoComparator = new MinCopyingComparator( MinSizeComparator.INSTANCE );
    }

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
        final List<NodeTaskLocalFilesAlignment> alignment = readyToRunToNode.createAlignmentForTasksWithAllDataOnNode(taskWithAllData, availableByNode);
        final ScheduleObject scheduleObject = new ScheduleObject( (List) alignment );
        scheduleObject.setCheckStillPossible( true );
        scheduleObject.setStopSubmitIfOneFails( true );
        return scheduleObject;
    }

    @Override
    void postScheduling( List<Task> unscheduledTasks, final Map<NodeWithAlloc, Requirements> availableByNode ) {
        final Map<NodeLocation, Integer> currentlyCopyingTasksOnNode = getCurrentlyCopying().getCurrentlyCopyingTasksOnNode();
        final List<NodeWithAlloc> allNodes = client.getAllNodes().stream().filter(
                                                    node -> currentlyCopyingTasksOnNode
                                                            .getOrDefault( node.getNodeLocation(), 0 ) < getMaxCopyTasksPerNode()
                                            ).collect( Collectors.toList() );
        final List<NodeTaskFilesAlignment> nodeTaskFilesAlignments;
        synchronized ( copyLock ) {
            final TaskStats taskStats = new TaskStats();
            //Calculate the stats of available data for each task and node.
            unscheduledTasks
                    .parallelStream()
                    .map( task -> {
                        final TaskInputs inputsOfTask = extractInputsOfData( task );
                        if ( inputsOfTask == null ) return null;
                        return getDataOnNode( task, inputsOfTask, allNodes );
                    } )
                    .filter( Objects::nonNull )
                    .sequential()
                    .forEach( taskStats::add );

            final CurrentlyCopying planedToCopy = new CurrentlyCopying();
            //Fill the currently available resources as fast as possible: start the tasks with the least data missing on a node.
            nodeTaskFilesAlignments = capacityAvailableToNode.createAlignmentForTasksWithEnoughCapacity( taskStats, planedToCopy, availableByNode, allNodes, getMaxCopyTasksPerNode() );
        }
        nodeTaskFilesAlignments.parallelStream().forEach( this::startCopyTask );
    }

    private void startCopyTask( final NodeTaskFilesAlignment nodeTaskFilesAlignment ) {
        final CopyTask copyTask = initializeCopyTask( nodeTaskFilesAlignment );
        //Files that will be copied
        reserveCopyTask( copyTask );
        try {
            copyRunner.startCopyTasks( copyTask, nodeTaskFilesAlignment );
        } catch ( Exception e ) {
            log.error( "Could not start copy task", e );
            undoReserveCopyTask( copyTask );
        }

    }


    /**
     * Creates config
     *
     * @param nodeTaskFilesAlignment
     * @return
     */
    CopyTask initializeCopyTask( NodeTaskFilesAlignment nodeTaskFilesAlignment ) {
        final CopyTask copyTask = createCopyTask(nodeTaskFilesAlignment);
        copyTask.setNodeLocation( nodeTaskFilesAlignment.node.getNodeLocation() );
        final List<LocationWrapper> allLocationWrappers = nodeTaskFilesAlignment.fileAlignment.getAllLocationWrappers();
        copyTask.setAllLocationWrapper( allLocationWrappers );
        log.info( "addToCopyingToNode task: {}, node: {}", nodeTaskFilesAlignment.task.getConfig().getName(), nodeTaskFilesAlignment.node.getName() );
        return copyTask;
    }

    private void reserveCopyTask( CopyTask copyTask ) {
        //Store files to copy
        addToCopyingToNode( copyTask.getTask(), copyTask.getNodeLocation(), copyTask.getFilesForCurrentNode() );
        useLocations( copyTask.getAllLocationWrapper() );
    }

    private void undoReserveCopyTask( CopyTask copyTask ) {
        //Store files to copy
        removeFromCopyingToNode( copyTask.getTask(), copyTask.getNodeLocation(), copyTask.getFilesForCurrentNode() );
        freeLocations( copyTask.getAllLocationWrapper() );
    }

    public void copyTaskFinished( CopyTask copyTask, boolean success ) {
        synchronized ( copyLock ) {
            freeLocations( copyTask.getAllLocationWrapper() );
            if( success ){
                    copyTask.getInputFiles().parallelStream().forEach( TaskInputFileLocationWrapper::success );
                    removeFromCopyingToNode( copyTask.getTask(), copyTask.getNodeLocation(), copyTask.getFilesForCurrentNode() );
            } else {
                    removeFromCopyingToNode( copyTask.getTask(), copyTask.getNodeLocation(), copyTask.getFilesForCurrentNode() );
                    handleProblematicCopy( copyTask );
            }
        }
    }

    private void handleProblematicCopy( CopyTask copyTask ){
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
        return new CopyTask( inputs, inputFiles, filesForCurrentNode, alignment.task );
    }

    List<NodeTaskFilesAlignment> createCopyTasks( List<DataOnNode> tasksAndData ){
        final CurrentlyCopying planedToCopy = new CurrentlyCopying();
        tasksAndData.sort( Comparator.comparing( DataOnNode::getNodesWithAllData ).reversed() );
        final Map<NodeLocation, Integer> assignedPodsByNode = copyToNodeManager.getCurrentlyCopyingTasksOnNode();
        List<NodeTaskFilesAlignment> nodeTaskAlignments = new LinkedList<>();
        for ( DataOnNode dataOnNode : tasksAndData ) {
            final Tuple<NodeWithAlloc, FileAlignment> result = calculateBestNode( dataOnNode, planedToCopy );
            if ( result != null && assignedPodsByNode.getOrDefault( result.getA().getNodeLocation(), 0 ) < getMaxCopyTasksPerNode() ) {
                addAlignmentToPlanned( planedToCopy, result.getB().getNodeFileAlignment(), dataOnNode.getTask(), result.getA() );
                nodeTaskAlignments.add( new NodeTaskFilesAlignment( result.getA(), dataOnNode.getTask(), result.getB() ) );
                assignedPodsByNode.put( result.getA().getNodeLocation(), assignedPodsByNode.getOrDefault( result.getA().getNodeLocation(), 0 ) + 1 );
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

        //logCopyTask.log( taskData.getTask().getConfig().getName() + " " + bestAlignment );

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


    /**
     * Calculate the remaining data on each node.
     * @param task
     * @param inputsOfTask
     * @param allNodes
     * @return A wrapper containing the remaining data on each node, the nodes where all data is available, the inputs and the task.
     */
    private TaskStat getDataOnNode( Task task, TaskInputs inputsOfTask, List<NodeWithAlloc> allNodes ) {
        TaskStat taskStats = new TaskStat( task, inputsOfTask, phaseTwoComparator );
        final CurrentlyCopying currentlyCopying = getCurrentlyCopying();
        for ( NodeWithAlloc node : allNodes ) {
            if ( !inputsOfTask.getExcludedNodes().contains( node.getNodeLocation() ) && affinitiesMatch( task.getPod(), node ) ) {
                final CurrentlyCopyingOnNode currentlyCopyingOnNode = currentlyCopying.get( node.getNodeLocation() );
                final TaskNodeStats taskNodeStats = inputsOfTask.calculateMissingData( node.getNodeLocation(), currentlyCopyingOnNode );
                if ( taskNodeStats != null ) {
                    taskStats.add( node, taskNodeStats );
                }
            }
        }
        taskStats.finish();
        return taskStats;
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
