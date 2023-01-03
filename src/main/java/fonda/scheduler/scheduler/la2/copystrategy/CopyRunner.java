package fonda.scheduler.scheduler.la2.copystrategy;

import fonda.scheduler.util.CopyTask;
import fonda.scheduler.util.NodeTaskFilesAlignment;

public interface CopyRunner {
    void startCopyTasks( CopyTask copyTask, NodeTaskFilesAlignment nodeTaskFilesAlignment );
}
