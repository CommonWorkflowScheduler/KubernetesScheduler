package cws.k8s.scheduler.scheduler;

import cws.k8s.scheduler.client.CWSKubernetesClient;
import cws.k8s.scheduler.model.SchedulerConfig;
import cws.k8s.scheduler.model.Task;
import cws.k8s.scheduler.model.cluster.*;
import cws.k8s.scheduler.scheduler.filealignment.InputAlignment;
import cws.k8s.scheduler.scheduler.la2.MinCopyingComparator;
import cws.k8s.scheduler.scheduler.la2.MinSizeComparator;
import cws.k8s.scheduler.scheduler.la2.TaskStat;
import cws.k8s.scheduler.scheduler.la2.ready2run.ReadyToRunToNode;
import cws.k8s.scheduler.util.NodeTaskFilesAlignment;

import java.io.File;
import java.util.List;

public class LocationAwareSchedulerGroups extends LocationAwareSchedulerV2 {

    GroupCluster groupCluster;

    public LocationAwareSchedulerGroups( String name, CWSKubernetesClient client, String namespace, SchedulerConfig config, InputAlignment inputAlignment, ReadyToRunToNode readyToRunToNode ) {
        super( name, client, namespace, config, inputAlignment, readyToRunToNode, null );
        groupCluster = new SimpleGroupCluster( hierarchyWrapper, client );
        readyToRunToNode.init( new FileSizeRankScoreGroup( groupCluster ) );
        setPhaseTwoComparator( new MinCopyingComparator( MinSizeComparator.INSTANCE ) );
        setPhaseThreeComparator( new RankAndMinCopyingComparatorCopyTasks( new MostOutLabelsComparator( groupCluster ) ) );
    }

    @Override
    protected void tasksWhereAddedToQueue( List<Task> newTasks ){
        super.tasksWhereAddedToQueue( newTasks );
        groupCluster.tasksBecameAvailable( newTasks );
    }

    @Override
    void taskWasScheduled(Task task ) {
        super.taskWasScheduled( task );
        groupCluster.taskWasAssignedToNode( task );
    }

    @Override
    int terminateTasks( List<Task> finishedTasks ) {
        final int terminatedTasks = super.terminateTasks( finishedTasks );
        groupCluster.tasksHaveFinished( finishedTasks );
        return terminatedTasks;
    }

    @Override
    List<TaskStat> getAdditionalTaskStatPhaseThree(){
        return groupCluster.getTaskStatToCopy(1);
    }

    void startCopyTask( final NodeTaskFilesAlignment nodeTaskFilesAlignment ) {
        if ( nodeTaskFilesAlignment.task instanceof CopyTask ) {
            final String workingDir = nodeTaskFilesAlignment.task.getWorkingDir();
            new File( workingDir ).mkdirs();
        }
        super.startCopyTask( nodeTaskFilesAlignment );
    }

}
