package fonda.scheduler.scheduler;

import fonda.scheduler.client.KubernetesClient;
import fonda.scheduler.model.*;
import fonda.scheduler.scheduler.copystrategy.CopyStrategy;
import fonda.scheduler.scheduler.copystrategy.FTPstrategy;
import io.fabric8.kubernetes.api.model.Node;
import io.fabric8.kubernetes.api.model.Pod;
import io.fabric8.kubernetes.client.Watcher;
import lombok.Getter;
import lombok.extern.slf4j.Slf4j;

import java.io.File;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.*;

@Slf4j
public abstract class SchedulerWithDaemonSet extends Scheduler {

    private final Map<String, String> daemonByNode = new HashMap<>();
    @Getter
    private final CopyStrategy copyStrategy;

    SchedulerWithDaemonSet(String execution, KubernetesClient client, String namespace, SchedulerConfig config) {
        super(execution, client, namespace, config);
        if ( config.copyStrategy == null ) throw new IllegalArgumentException( "Copy strategy is null" );
        switch ( config.copyStrategy ){
            case "ftp":
            case "copy":
                copyStrategy = new FTPstrategy();
                break;
            default:
                throw new IllegalArgumentException( "Copy strategy is unknown " + config.copyStrategy );
        }
    }

    public String getDaemonOnNode( String node ){
        synchronized ( daemonByNode ) {
            return daemonByNode.get( node );
        }
    }

    String getDaemonOnNode( Node node ){
        return getDaemonOnNode( node.getMetadata().getName() );
    }
    
    String getOneDaemon() {
        synchronized ( daemonByNode ) {
            final Collection<String> values = daemonByNode.values();
            return values.stream().skip(values.size()).findFirst().get();
        }
    }

    void uploadDataToNode ( Node node, File file, Path dest ) {
        int i = 0;
        do {
            log.info( "Upload {} to {} ({})", file, dest, node.getMetadata().getName() );
            try {
                final boolean result = client.pods()
                        .inNamespace(getNamespace())
                        .withName(getDaemonOnNode(node))
                        .file(dest.toString())
                        .upload(file.toPath());
                if (result) return;
            } catch (Exception e){
                e.printStackTrace();
            }
            try {
                Thread.sleep((long) (Math.pow(2,i) * 100) );
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
        } while ( i++ < 10 );
        throw new RuntimeException( "Can not upload file: " + file + " to node: " + node.getMetadata().getName() );
    }

    @Override
    void assignTaskToNode(Task task, NodeWithAlloc node ) {

        getCopyStrategy().generateCopyScript( task );

        task.setNode( node.getNodeLocation() );

        super.assignTaskToNode(task, node);
    }

    @Override
    int terminateTasks(List<Task> finishedTasks) {
        final TaskResultParser taskResultParser = new TaskResultParser();
        finishedTasks.parallelStream().forEach( finishedTask -> {
            final Set<PathLocationWrapperPair> newAndUpdatedFiles = taskResultParser.getNewAndUpdatedFiles(
                    Paths.get(finishedTask.getWorkingDir()),
                    finishedTask.getNode(),
                    finishedTask.getProcess()
            );
            for (PathLocationWrapperPair newAndUpdatedFile : newAndUpdatedFiles) {
                hierarchyWrapper.addFile( newAndUpdatedFile.getPath(), newAndUpdatedFile.getLocationWrapper() );
            }
            super.taskWasFinished( finishedTask );
        });
        return 0;
    }

    @Override
    void podEventReceived(Watcher.Action action, Pod pod){
        while ( daemonByNode == null ){
            //The Watcher can be started before the class is initialized
            try {
                Thread.sleep(20);
            } catch (InterruptedException e) {}
        }
        if( ( "mount-" + this.getExecution().replace('_', '-') + "-" ).equals(pod.getMetadata().getGenerateName()) ){
            final String nodeName = pod.getSpec().getNodeName();
            if ( nodeName != null ){
                synchronized ( daemonByNode ) {
                    final String podName = pod.getMetadata().getName();
                    final boolean podIsCurrentDaemon = podName.equals(daemonByNode.get(nodeName));
                    if ( action == Watcher.Action.DELETED ) {
                        if (podIsCurrentDaemon) {
                            daemonByNode.remove(nodeName);
                        }
                    } else if ( pod.getStatus().getPhase().equals("Running") ) {
                        daemonByNode.put( nodeName, pod.getStatus().getPodIP());
                        informResourceChange();
                    } else if ( podIsCurrentDaemon ) {
                        daemonByNode.remove(nodeName);
                        if( !pod.getStatus().getPhase().equals("Failed") ){
                            log.info( "Unexpected phase {} for daemon: {}", pod.getStatus().getPhase(), podName );
                        }
                    }
                }
            }
        }
    }

}
