package cws.k8s.scheduler.util.score;

import cws.k8s.scheduler.model.location.hierachy.HierarchyWrapper;
import cws.k8s.scheduler.scheduler.data.TaskInputsNodes;

public class FileSizeRankScore extends FileSizeScore {

    public FileSizeRankScore( HierarchyWrapper hierarchyWrapper ) {
        super( hierarchyWrapper );
    }

    @Override
    public long getScore( TaskInputsNodes taskInputsNodes ) {
        //Add one to avoid becoming zero
        final int rank = taskInputsNodes.getTask().getProcess().getRank() + 1;
        final long rankFactor = 100_000_000_000_000L * rank; // long would allow a rank of 92233
        return super.getScore( taskInputsNodes ) + rankFactor ;
    }

}
