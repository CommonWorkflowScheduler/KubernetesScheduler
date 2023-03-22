package cws.k8s.scheduler.scheduler.la2.copystrategy;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import cws.k8s.scheduler.client.KubernetesClient;
import cws.k8s.scheduler.scheduler.LocationAwareSchedulerV2;
import cws.k8s.scheduler.util.CopyTask;
import cws.k8s.scheduler.util.LogCopyTask;
import cws.k8s.scheduler.util.NodeTaskFilesAlignment;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;

import java.io.IOException;
import java.nio.file.Path;

@Slf4j
@RequiredArgsConstructor
public class ShellCopy implements CopyRunner {

    private final KubernetesClient client;
    private final LocationAwareSchedulerV2 scheduler;
    private final LogCopyTask logCopyTask;

    @Override
    public void startCopyTasks( final CopyTask copyTask, final NodeTaskFilesAlignment nodeTaskFilesAlignment ) {
        final String nodeName = nodeTaskFilesAlignment.node.getName().replace( " ", "_" );
        String copyTaskIdentifier = nodeName + "-" + copyTask.getTask().getCurrentCopyTaskId();
        String filename = ".command.init." + copyTaskIdentifier + ".json";
        String[] command = new String[3];
        command[0] = "/bin/bash";
        command[1] = "-c";
        command[2] = "cd " + nodeTaskFilesAlignment.task.getWorkingDir() + " && ";
        try {
            new ObjectMapper().writeValue( Path.of( nodeTaskFilesAlignment.task.getWorkingDir(), filename ).toFile(), copyTask.getInputs() );
        } catch ( JsonProcessingException e ) {
            throw new RuntimeException( e );
        } catch ( IOException e ) {
            throw new RuntimeException( e );
        }
        command[2] += "/code/ftp.py false \"" + copyTaskIdentifier + "\" \"" + filename + "\"";
        String name = nodeTaskFilesAlignment.task.getConfig().getName() + "-copy-" + nodeTaskFilesAlignment.node.getName();
        log.info( "Starting {} to node {}", nodeTaskFilesAlignment.task.getConfig().getName(), nodeTaskFilesAlignment.node.getName() );
        logCopyTask.copy( nodeTaskFilesAlignment.task.getConfig().getName(), nodeTaskFilesAlignment.node.getName(), copyTask.getInputFiles().size(), "start" );
        client.execCommand( scheduler.getDaemonNameOnNode( copyTask.getNodeLocation().getIdentifier() ), scheduler.getNamespace(), command, new LaListener( copyTask, name, nodeTaskFilesAlignment, scheduler, logCopyTask ) );
    }

}
