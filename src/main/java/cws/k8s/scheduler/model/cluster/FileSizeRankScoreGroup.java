package cws.k8s.scheduler.model.cluster;

import cws.k8s.scheduler.model.Task;
import cws.k8s.scheduler.model.location.NodeLocation;
import cws.k8s.scheduler.util.score.FileSizeRankScore;
import lombok.RequiredArgsConstructor;

@RequiredArgsConstructor
public class FileSizeRankScoreGroup extends FileSizeRankScore {

    private final GroupCluster groupCluster;

    @Override
    public long getScore( Task task, NodeLocation location, long size ) {
        final long score = super.getScore( task, location, size );
        final long newScore = (long) (score * groupCluster.getScoreForTaskOnNode( task, location ));
        if ( newScore < 1 ) {
            return 1;
        }
        return newScore;
    }

}
