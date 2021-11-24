package fonda.scheduler.scheduler;

import com.fasterxml.jackson.databind.ObjectMapper;
import fonda.scheduler.client.KubernetesClient;
import fonda.scheduler.model.*;
import fonda.scheduler.model.location.hierachy.Folder;
import fonda.scheduler.model.location.hierachy.HierarchyWrapper;
import fonda.scheduler.model.location.hierachy.RealFile;
import fonda.scheduler.scheduler.copystrategy.CopyStrategy;
import fonda.scheduler.scheduler.copystrategy.FTPstrategy;
import fonda.scheduler.scheduler.schedulingstrategy.InputEntry;
import fonda.scheduler.scheduler.schedulingstrategy.Inputs;
import fonda.scheduler.scheduler.util.NodeTaskAlignment;
import fonda.scheduler.scheduler.util.NodeTaskFilesAlignment;
import fonda.scheduler.scheduler.util.PathFileLocationTriple;
import fonda.scheduler.util.FilePath;
import io.fabric8.kubernetes.api.model.Node;
import io.fabric8.kubernetes.api.model.Pod;
import io.fabric8.kubernetes.client.Watcher;
import lombok.Getter;
import lombok.extern.slf4j.Slf4j;

import java.io.File;
import java.io.IOException;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.*;
import java.util.stream.Collectors;
import java.util.stream.Stream;

@Slf4j
public abstract class SchedulerWithDaemonSet extends Scheduler {

    private final Map<String, String> daemonByNode = new HashMap<>();
    @Getter
    private final CopyStrategy copyStrategy;
    final HierarchyWrapper hierarchyWrapper;

    SchedulerWithDaemonSet(String execution, KubernetesClient client, String namespace, SchedulerConfig config) {
        super(execution, client, namespace, config);
        this.hierarchyWrapper = new HierarchyWrapper( config.workDir );
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
    
    @Override
    void assignTaskToNode( NodeTaskAlignment alignment ) {
        writeInitConfig( (NodeTaskFilesAlignment) alignment );
        getCopyStrategy().generateCopyScript( alignment.task );
        super.assignTaskToNode( alignment );
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

    boolean writeInitConfig( NodeTaskFilesAlignment alignment ) {

        final File config = new File(alignment.task.getWorkingDir() + '/' + ".command.inputs.json");
        try {
            final Inputs inputs = new Inputs(
                    this.getDns() + "/daemon/" + getNamespace() + "/" + getExecution() + "/"
            );
            for (Map.Entry<String, List<FilePath>> entry : alignment.nodeFileAlignment.entrySet()) {
                if( entry.getKey().equals( alignment.node.getMetadata().getName() ) ) continue;
                final List<String> collect = entry  .getValue()
                                                    .stream()
                                                    .map(x -> x.path)
                                                    .collect(Collectors.toList());
                inputs.data.add( new InputEntry( getDaemonOnNode( entry.getKey() ), entry.getKey(), collect ) );
            }
            new ObjectMapper().writeValue( config, inputs );
            return true;
        } catch (IOException e) {
            e.printStackTrace();
        }
        return false;
    }

    private Stream<PathFileLocationTriple> streamFile(
                final fonda.scheduler.model.location.hierachy.File file,
                final Task task,
                final Path sourcePath )
    {
        if( file.isDirectory() ){
            return ((Folder) file).getAllChildren( sourcePath )
                    .entrySet()
                    .stream()
                    .map( y -> new PathFileLocationTriple(
                                    y.getKey(),
                                    y.getValue(),
                                    y.getValue().getFilesForProcess( task.getProcess() )
                            )
                    );
        }
        final RealFile realFile = (RealFile) file;
        return Stream.of( new PathFileLocationTriple(
                        sourcePath,
                        realFile,
                        realFile.getFilesForProcess( task.getProcess() )
                )
        );
    }

    List<PathFileLocationTriple> getInputsOfTask( Task task ){
        return task.getConfig()
                .getInputs()
                .parallelStream()
                .flatMap(x -> {
                    if ( !(x.getValue() instanceof List) ) return Stream.empty();
                    for (Object o : (List) x.getValue()) {
                        if (o instanceof Map) {
                            Map<String, Object> input = (Map<String, Object>) o;
                            if (input.containsKey("sourceObj")) {
                                String sourceObj = (String) input.get("sourceObj");
                                Path sourcePath = Paths.get(sourceObj);
                                log.info("Src: {}", sourceObj);
                                if (this.hierarchyWrapper.isInScope(sourcePath)) {
                                    return streamFile( hierarchyWrapper.getFile(sourcePath), task, sourcePath );
                                }
                            }
                        }
                    }
                    return Stream.empty();
                })
                .collect(Collectors.toList());
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
