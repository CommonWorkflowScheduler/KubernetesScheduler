package cws.k8s.scheduler.model.cluster;

import cws.k8s.scheduler.client.KubernetesClient;
import cws.k8s.scheduler.model.NodeWithAlloc;
import cws.k8s.scheduler.model.Task;
import cws.k8s.scheduler.model.location.hierachy.HierarchyWrapper;
import cws.k8s.scheduler.model.location.hierachy.NoAlignmentFoundException;
import cws.k8s.scheduler.model.location.hierachy.RealHierarchyFile;
import cws.k8s.scheduler.model.outfiles.PathLocationWrapperPair;
import cws.k8s.scheduler.model.taskinputs.PathFileLocationTriple;
import cws.k8s.scheduler.model.taskinputs.TaskInputs;
import cws.k8s.scheduler.scheduler.la2.TaskStat;
import cws.k8s.scheduler.util.TaskNodeStats;
import lombok.Getter;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;

import java.nio.file.Path;
import java.util.*;
import java.util.stream.Collectors;

@Slf4j
@RequiredArgsConstructor
public abstract class GroupCluster {

    private final double minScoreToCopy = 0.5;
    // Group all tasks by label and deliver statistics
    protected final Map<String,LabelCount> countPerLabel = new HashMap<>();
    // tasks that are not yet scheduled
    protected final LinkedList<Task> unscheduledTasks = new LinkedList<>();
    // tasks that are assigned to a task and running
    protected final LinkedList<Task> scheduledTasks = new LinkedList<>();
    // tasks that have finished
    protected final LinkedList<Task> finishedTasks = new LinkedList<>();
    // Map from label to node, which node is responsible for the label
    protected final Map<String, NodeWithAlloc> labelToNode = new HashMap<>();
    // Map from node to label, which labels are assigned to the node
    protected final Map<NodeWithAlloc,Set<String>> nodeToLabel = new HashMap<>();
    @Getter
    private final HierarchyWrapper hierarchyWrapper;
    @Getter
    private final KubernetesClient client;

    /**
     * When new tasks become available and are ready to schedule, this method is called to register them.
     * @param tasks the tasks that are ready to schedule
     */
    public void tasksBecameAvailable( List<Task> tasks ) {
        boolean anyWithLabel = false;
        synchronized ( this ) {
            unscheduledTasks.addAll( tasks );
            for ( Task task : tasks ) {
                if ( task.getOutLabel() == null || task.getOutLabel().isEmpty() ) {
                    continue;
                }
                for ( String label : task.getOutLabel() ) {
                    anyWithLabel = true;
                    final LabelCount labelCount = countPerLabel.computeIfAbsent( label, LabelCount::new );
                    labelCount.addWaitingTask( task );
                }
            }
            // If we add at least one task with an outLabel we need to recalculate our alignment
            if ( anyWithLabel ) {
                recalculate();
            }
        }
    }

    /**
     * This method is called when a task was scheduled to a node and can no longer be scheduled to another node.
     * @param task the task that was scheduled
     */
    public void taskWasAssignedToNode( Task task ) {
        synchronized ( this ) {
            unscheduledTasks.remove( task );
            scheduledTasks.add( task );
            if ( task.getOutLabel() != null ) {
                for ( String label : task.getOutLabel() ) {
                    countPerLabel.get( label ).makeTaskRunning( task );
                }
                recalculate();
            }
        }
    }

    /**
     * This method is called when a task has finished and output data can be considered.
     * @param tasks the tasks that have finished
     */
    public void tasksHaveFinished( List<Task> tasks ) {
        synchronized ( this ) {
            scheduledTasks.removeAll( tasks );
            finishedTasks.addAll( tasks );
            for ( Task task : tasks ) {
                if ( task.getOutLabel() == null ) {
                    continue;
                }
                for ( String label : task.getOutLabel() ) {
                    countPerLabel.get( label ).makeTaskFinished( task );
                }
            }
        }
    }

    /**
     * This method is called when the state changes.
     * This is either the case when a new task becomes available, or a task is scheduled.
     */
    abstract void recalculate();

    /**
     * Return the score for a task on a node.
     * This calculates the score using jaccard similarity coefficient.
     * Therefore, it calculates the shared labels between the task and the node and divides it by the total amount of labels of the task.
     * @param task the task
     * @param node the node
     * @return the score in the range of 0 to 1, where 1 is the best score
     */
    public double getScoreForTaskOnNode( Task task, NodeWithAlloc node ) {
        // If the task has no outLabel, it can run on any node
        final Set<String> outLabel = task.getOutLabel();
        if ( outLabel == null || outLabel.isEmpty() ) {
            return 1;
        }

        final Set<String> nodeLabels = nodeToLabel.get( node );
        return calculateJaccardSimilarityCoefficient( outLabel, nodeLabels );
    }

    /**
     * Calculate the Jaccard similarity coefficient between two sets of labels.
     * If there is no overlap, the coefficient is 0.01.
     * @param outLabel the first set of labels, this cannot be empty or null
     * @param nodeLabels the second set of labels, this can be null or empty
     * @return the Jaccard similarity coefficient in the range of 0.01 to 1
     */
    static double calculateJaccardSimilarityCoefficient( Set<String> outLabel, Set<String> nodeLabels ) {
        if ( outLabel == null || outLabel.isEmpty() ) {
            throw new IllegalArgumentException( "outLabel cannot be empty or null" );
        }
        final double lowerBound = 0.01;
        if ( nodeLabels == null || nodeLabels.isEmpty() ) {
            return lowerBound;
        }
        outLabel = new HashSet<>( outLabel );
        int outLabelSize = outLabel.size();
        //intersection of both sets.
        outLabel.retainAll( nodeLabels );
        // No common labels
        if ( outLabelSize == 0 ) {
            return lowerBound;
        }
        final double result = (double) outLabel.size() / outLabelSize;
        return Math.max( lowerBound, Math.min( 1, result )); // Clamp the result between 0.01 and 1
    }

    /**
     * Assign a node to a label, such that tasks for this label can be prepared on the node.
     * @param nodeLocation the node
     * @param label the label
     */
    protected void addNodeToLabel( NodeWithAlloc nodeLocation, String label ) {
        final NodeWithAlloc currentLocation = labelToNode.get( label );

        if ( nodeLocation.equals( currentLocation ) ) {
            return;
        }

        Set<String> labels = nodeToLabel.computeIfAbsent( nodeLocation, k -> new HashSet<>() );
        labels.add( label );

        if ( currentLocation != null ) {
            labels = nodeToLabel.get( currentLocation );
            labels.remove( label );
        }
        labelToNode.put( label, nodeLocation );
    }

    /**
     * Create TaskStat for tasks that could be prepared on a node, return maximal {@code maxCopiesPerNode} TaskStat per node.
     * Only tasks with a score higher than {@link #minScoreToCopy} are considered.
     * @param maxCopiesPerNode the maximum amount of copies that will be created for a node
     * @return a list of all TaskStats that should be copied
     */
    public List<TaskStat> getTaskStatToCopy( final int maxCopiesPerNode ){
        synchronized ( this ) {
            final Map<NodeWithAlloc,Integer> tasksForLocation = new HashMap<>();
            //all tasks where score > minScoreToCopy and tasks have at least one outLabel
            final List<DataMissing> tasksThatNeedToBeCopied = getTasksThatNeedToBeCopied();
            List<TaskStat> taskStats = new ArrayList<>();
            for ( DataMissing dataMissing : tasksThatNeedToBeCopied ) {
                Integer currentCopies = tasksForLocation.getOrDefault( dataMissing.getNode(), 0 );
                // Only create maxCopiesPerNode possible TaskStats per node
                TaskStat taskStat = currentCopies < maxCopiesPerNode ? getTaskStat( dataMissing ) : null;
                // TaskStat is null if the output data was requested for a real task already and should not be copied anymore
                if ( taskStat != null ) {
                   taskStats.add( taskStat );
                   tasksForLocation.put( dataMissing.getNode(), ++currentCopies );
                }
            }
            return taskStats;
        }
    }

    /**
     * Create a TaskStat for a {@link DataMissing} object.
     * @param dataMissing the DataMissing object
     * @return the TaskStat or null if the output data was requested for a real task already
     */
    private TaskStat getTaskStat( DataMissing dataMissing ) {
        final NodeWithAlloc node = dataMissing.getNode();
        final OutputFiles outputFilesWrapper = dataMissing.getTask().getOutputFiles();
        final Set<PathLocationWrapperPair> outputFiles = outputFilesWrapper.getFiles();
        // Do not check for OutputFiles#wasRequestedForRealTask() here,
        // because it is checked when the DataMissing object is created in LabelCount.tasksNotOnNode
        List<PathFileLocationTriple> files = outputFiles
                .parallelStream()
                .map( x -> convertToPathFileLocationTriple(x, dataMissing.getTask()) )
                .collect( Collectors.toList() );
        // If in the previous step at least one file was requested for a real task,
        // all the output data is considered as requested for a real task
        if ( dataMissing.getTask().getOutputFiles().isWasRequestedForRealTask() ) {
            return null;
        }
        TaskInputs inputsOfTask = new FakeTaskInputs( files, node.getNodeLocation() );
        final CopyTask copyTask = new CopyTask( dataMissing.getTask(), node, dataMissing.getLabelCounts() );
        final TaskStat taskStat = new TaskStat( copyTask, inputsOfTask );
        // TaskNodeStats is created with the total size and 0 for the data that is on the node
        final TaskNodeStats taskNodeStats = new TaskNodeStats( inputsOfTask.calculateAvgSize(), 0, 0 );
        taskStat.add( dataMissing.getNode(), taskNodeStats );
        return taskStat;
    }

    /**
     * Convert a PathLocationWrapperPair to a PathFileLocationTriple.
     * Check for every file if it was requested for a real task.
     * @param pair the PathLocationWrapperPair containing the LocationWrapper to process
     * @param task the task that created the output data
     * @return the PathFileLocationTriple or null if the output data was requested for a real task
     */
    private PathFileLocationTriple convertToPathFileLocationTriple( PathLocationWrapperPair pair, Task task ){
        final Path path = pair.getPath();
        RealHierarchyFile file = (RealHierarchyFile) hierarchyWrapper.getFile( path );
        // if at least one file was requested for a real task, all the output data is considered as requested for a real task
        // return null as we ignore this output data for proactively copying
        if ( file.wasRequestedByTask() ) {
            task.getOutputFiles().wasRequestedForRealTask();
            return null;
        }
        try {
            // task is the task that created the output data
            final RealHierarchyFile.MatchingLocationsPair filesForTask = file.getFilesForTask( task );
            return new PathFileLocationTriple( path, file, filesForTask.getMatchingLocations() );
        } catch ( NoAlignmentFoundException e ) {
            throw new RuntimeException( e );
        }
    }

    /**
     * Get all MissingData for tasks.
     * This method checks for each task if the output data is on the node that is responsible for the label.
     * If the data is not on the node, a DataMissing object is created. But only if the score is higher than {@link #minScoreToCopy}.
     * @return a stream of DataMissing objects
     */
    private List<DataMissing> getTasksThatNeedToBeCopied(){
        final Map<Map.Entry<Task, NodeWithAlloc>, List<LabelCount>> collect = countPerLabel.entrySet()
                .parallelStream()
                .unordered()
                .flatMap( v -> {
                    final String label = v.getKey();
                    final LabelCount value = v.getValue();
                    // which node is responsible for the label
                    final NodeWithAlloc node = labelToNode.get( label );
                    // get missing data for all tasks that are not on the node
                    return value.tasksNotOnNode( node );
                } )
                // Group all Labels for DataMissingIntern with the same task and node into a map
                .collect( Collectors.groupingBy(
                        dmi -> Map.entry( dmi.getTask(), dmi.getNode() ),
                        Collectors.mapping( DataMissingIntern::getLabelCount, Collectors.toList() )
                ) );
        List<DataMissing> result = new ArrayList<>( collect.size() );
        for ( Map.Entry<Map.Entry<Task, NodeWithAlloc>, List<LabelCount>> e : collect.entrySet() ) {
            final Task task = e.getKey().getKey();
            final NodeWithAlloc node = e.getKey().getValue();
            final List<LabelCount> labelCounts = e.getValue();
            final double score = getScoreForTaskOnNode( task, node );
            // Only return tasks that have a score higher than minScoreToCopy
            if ( score > minScoreToCopy ) {
                result.add( new DataMissing( task, node, labelCounts, score ) );
            }
        }
        // Sort the tasks by score in descending order: highest score first
        result.sort( (x, y) -> Double.compare( y.getScore(), x.getScore() ) );
        return result;
    }

}
