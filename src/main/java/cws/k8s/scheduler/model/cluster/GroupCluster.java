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
import java.util.stream.Stream;

@Slf4j
@RequiredArgsConstructor
public abstract class GroupCluster {

    private final double minScoreToCopy = 0.5;
    protected final Set<String> allLabels = new HashSet<>();
    protected final Map<String,LabelCount> countPerLabel = new HashMap<>();
    protected final LinkedList<Task> unscheduledTasks = new LinkedList<>();
    protected final LinkedList<Task> scheduledTasks = new LinkedList<>();
    protected final LinkedList<Task> finishedTasks = new LinkedList<>();
    protected final Map<String, NodeWithAlloc> labelToNode = new HashMap<>();
    protected final Map<NodeWithAlloc,Set<String>> nodeToLabel = new HashMap<>();
    @Getter
    private final HierarchyWrapper hierarchyWrapper;
    @Getter
    private final KubernetesClient client;

    private void addAllOutLabels( List<Task> tasks ) {
        for ( Task task : tasks ) {
            if ( task.getOutLabel() != null ) {
                allLabels.addAll( task.getOutLabel() );
            }
        }
    }

    public void tasksBecameAvailable( List<Task> tasks ) {
        boolean anyWithLabel = false;
        synchronized ( this ) {
            unscheduledTasks.addAll( tasks );
            addAllOutLabels( tasks );
            for ( Task task : tasks ) {
                if ( task.getOutLabel() == null || task.getOutLabel().isEmpty() ) {
                    continue;
                }
                for ( String label : task.getOutLabel() ) {
                    anyWithLabel = true;
                    countPerLabel.computeIfAbsent( label, k -> new LabelCount(k) ).addWaitingTask( task );
                    final LabelCount labelCount;
                    if ( !countPerLabel.containsKey( label ) ) {
                        labelCount = new LabelCount( label );
                        countPerLabel.put( label, labelCount );
                    } else {
                        labelCount = countPerLabel.get( label );
                    }
                    labelCount.addWaitingTask( task );
                }
            }
            if ( anyWithLabel ) {
                recalculate();
            }
        }
    }

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

    public void tasksHaveFinished( List<Task> tasks ) {
        synchronized ( this ) {
            scheduledTasks.removeAll( tasks );
            finishedTasks.addAll( tasks );
            for ( Task task : tasks ) {
                if ( task.getOutLabel() == null ) {
                    continue;
                }
                for ( String label : task.getOutLabel() ) {
                    if ( countPerLabel.get( label ) == null ) {
                        continue;
                    }
                    countPerLabel.get( label ).makeTaskFinished( task );
                }
            }
        }
    }

    abstract void recalculate();

    public double getScoreForTaskOnNode( Task task, NodeWithAlloc node ) {
        if ( task.getOutLabel() == null || task.getOutLabel().isEmpty() ) {
            return 1;
        }
        //Jaccard similarity coefficient
        final Set<String> outLabel = new HashSet<>( task.getOutLabel() );
        int outLabelSize = outLabel.size();
        final Set<String> nodeLabels = nodeToLabel.get( node );
        if ( nodeLabels == null ) {
            return 0.01;
        }
        outLabel.retainAll( nodeLabels );
        if ( outLabelSize == 0 ) {
            return 1;
        }
        return (double) outLabel.size() / outLabelSize + 0.01; // to avoid zero
    }

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
     * Return a list of all tasks for that a node exists where getScore is higher than minScoreToCopy and the task has at least one outLabel
     * @return
     */
    public List<TaskStat> getTaskStatToCopy(){
        int maxCopiesPerNode = 1;
        synchronized ( this ) {
            //all tasks where score > minScoreToCopy and tasks have at least one outLabel
            final Map<NodeWithAlloc,Integer> taskForLocation = new HashMap<>();
            return getTasksThatNeedToBeCopied()
                    .sequential()
                    //Only create maxCopiesPerNode possible copy tasks
                    .filter( x -> shouldStillCreate( x, taskForLocation, maxCopiesPerNode ) )
                    .map( this::getTaskStat )
                    .filter( Objects::nonNull )
                    .collect( Collectors.toList());
        }
    }

    private static boolean shouldStillCreate( DataMissing x, Map<NodeWithAlloc, Integer> taskForLocation, int maxCopiesPerNode ) {
        if ( taskForLocation.containsKey( x.getNode() ) ) {
            final Integer i = taskForLocation.get( x.getNode() );
            if ( i == maxCopiesPerNode ) {
                return false;
            } else {
                taskForLocation.put( x.getNode(), i + 1 );
                return true;
            }
        } else {
            taskForLocation.put( x.getNode(), 1 );
            return true;
        }
    }

    private TaskStat getTaskStat( DataMissing dataMissing ) {
        final NodeWithAlloc node = dataMissing.getNode();
        final OutputFiles outputFilesWrapper = dataMissing.getTask().getOutputFiles();
        final Set<PathLocationWrapperPair> outputFiles = outputFilesWrapper.getFiles();
        List<PathFileLocationTriple> files = outputFiles
                .parallelStream()
                .map( x -> convertToPathFileLocationTriple(x, dataMissing.getTask()) )
                .collect( Collectors.toList() );
        if ( dataMissing.getTask().getOutputFiles().isWasRequestedForRealTask() ) {
            return null;
        }
        TaskInputs inputsOfTask = new FakeTaskInputs( files, node.getNodeLocation() );
        final CopyTask copyTask = new CopyTask( dataMissing.getTask(), node, dataMissing.getLabelCount() );
        final TaskStat taskStat = new TaskStat( copyTask, inputsOfTask );
        final TaskNodeStats taskNodeStats = new TaskNodeStats( inputsOfTask.calculateAvgSize(), 0, 0 );
        //client.
        taskStat.add( dataMissing.getNode(), taskNodeStats );
        return taskStat;
    }

    private PathFileLocationTriple convertToPathFileLocationTriple( PathLocationWrapperPair pair, Task task ){
        final Path path = pair.getPath();
        RealHierarchyFile file = (RealHierarchyFile) hierarchyWrapper.getFile( path );
        final RealHierarchyFile.MatchingLocationsPair filesForTask;
        if ( file.wasRequestedByTask() ) {
            task.getOutputFiles().wasRequestedForRealTask();
            return null;
        }
        try {
            filesForTask = file.getFilesForTask( task );
        } catch ( NoAlignmentFoundException e ) {
            throw new RuntimeException( e );
        }
        return new PathFileLocationTriple( path, file, filesForTask.getMatchingLocations() );
    }

    private Stream<DataMissing> getTasksThatNeedToBeCopied(){
        return countPerLabel.entrySet()
                .parallelStream()
                .unordered()
                .flatMap( v -> {
                    final String label = v.getKey();
                    final LabelCount value = v.getValue();
                    final NodeWithAlloc node = labelToNode.get( label );
                    return value.taskNotOnNode( node );
                } )
                .distinct()
                .filter( d -> getScoreForTaskOnNode( d.getTask(), d.getNode() ) > minScoreToCopy )
                .sorted( (x, y) -> Double.compare( getScoreForTaskOnNode( y.getTask(), y.getNode() ),
                        getScoreForTaskOnNode( x.getTask(), x.getNode() ) ) );
    }

}
