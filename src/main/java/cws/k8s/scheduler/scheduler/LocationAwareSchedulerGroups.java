package cws.k8s.scheduler.scheduler;

import cws.k8s.scheduler.client.KubernetesClient;
import cws.k8s.scheduler.model.SchedulerConfig;
import cws.k8s.scheduler.model.Task;
import cws.k8s.scheduler.model.cluster.FileSizeRankScoreGroup;
import cws.k8s.scheduler.model.cluster.GroupCluster;
import cws.k8s.scheduler.model.cluster.MostOutLabelsComparator;
import cws.k8s.scheduler.model.cluster.SimpleGroupCluster;
import cws.k8s.scheduler.scheduler.filealignment.InputAlignment;
import cws.k8s.scheduler.scheduler.la2.MaxSizeComparator;
import cws.k8s.scheduler.scheduler.la2.MinCopyingComparator;
import cws.k8s.scheduler.scheduler.la2.MinSizeComparator;
import cws.k8s.scheduler.scheduler.la2.RankAndMinCopyingComparator;
import cws.k8s.scheduler.scheduler.la2.ready2run.ReadyToRunToNode;
import cws.k8s.scheduler.util.score.FileSizeRankScore;

import java.util.List;

public class LocationAwareSchedulerGroups extends LocationAwareSchedulerV2 {

    GroupCluster groupCluster;

    public LocationAwareSchedulerGroups( String name, KubernetesClient client, String namespace, SchedulerConfig config, InputAlignment inputAlignment, ReadyToRunToNode readyToRunToNode ) {
        super( name, client, namespace, config, inputAlignment, readyToRunToNode, null );
        groupCluster = new SimpleGroupCluster( client );
        readyToRunToNode.init( new FileSizeRankScoreGroup( groupCluster ) );
        setPhaseTwoComparator( new MinCopyingComparator( MinSizeComparator.INSTANCE ) );
        setPhaseThreeComparator( new RankAndMinCopyingComparator( new MostOutLabelsComparator( groupCluster ) ) );
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
}
