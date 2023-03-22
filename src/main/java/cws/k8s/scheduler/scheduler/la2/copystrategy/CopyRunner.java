package cws.k8s.scheduler.scheduler.la2.copystrategy;

import cws.k8s.scheduler.util.CopyTask;
import cws.k8s.scheduler.util.NodeTaskFilesAlignment;

public interface CopyRunner {
    void startCopyTasks( CopyTask copyTask, NodeTaskFilesAlignment nodeTaskFilesAlignment );
}
