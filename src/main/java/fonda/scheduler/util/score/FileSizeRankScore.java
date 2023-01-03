package fonda.scheduler.util.score;

import fonda.scheduler.model.location.hierachy.HierarchyWrapper;
import fonda.scheduler.scheduler.data.TaskInputsNodes;

public class FileSizeRankScore extends FileSizeScore {

    public FileSizeRankScore( HierarchyWrapper hierarchyWrapper ) {
        super( hierarchyWrapper );
    }

    @Override
    public long getScore( TaskInputsNodes taskInputsNodes ) {
        return super.getScore( taskInputsNodes ) + taskInputsNodes.getTask().getProcess().getRank() * 100_000_000_000L;
    }

}
