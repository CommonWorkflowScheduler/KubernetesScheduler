package cws.k8s.scheduler.model.cluster;

import cws.k8s.scheduler.dag.Process;
import cws.k8s.scheduler.model.NodeWithAlloc;
import cws.k8s.scheduler.model.Requirements;
import cws.k8s.scheduler.model.Task;
import cws.k8s.scheduler.model.TaskConfig;
import lombok.extern.slf4j.Slf4j;

import java.nio.file.Path;

@Slf4j
public class CopyTask extends Task {

    private final static Process COPY_PROCESS = new Process( "Copy", Integer.MAX_VALUE );
    private final Task task;
    private final NodeWithAlloc node;
    private final LabelCount labelCount;

    protected CopyTask( Task task, NodeWithAlloc node, LabelCount labelCount ) {
        super( new TaskConfig("Copy","Copy-" + task.getConfig().getName() + " -> " + node.getNodeLocation()
                + " (" + labelCount.getLabel() + ")" , buildCopyTaskFolder(task), task.getConfig().getRunName() ), COPY_PROCESS );
        this.task = task;
        this.node = node;
        this.labelCount = labelCount;
    }

    private static String buildCopyTaskFolder( Task task ) {
        final Path path = Path.of( task.getConfig().getWorkDir() );
        Path p = Path.of(
                path.getParent().getParent().toString(),
                "copy",
                path.getParent().getFileName().toString(),
                path.getFileName().toString()
        );
        return p.toString();
    }

    @Override
    public Requirements getRequest() {
        return new Requirements( 0, 0 );
    }

    @Override
    public boolean equals( Object obj ) {
        if ( obj instanceof CopyTask ) {
            return this.task.equals( ((CopyTask) obj).task );
        }
        else return false;
    }

    @Override
    public int hashCode() {
        return task.hashCode() + 5;
    }

    public void finished(){
        labelCount.taskIsNowOnNode( task, node );
    }
}
