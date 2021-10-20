package fonda.scheduler.scheduler;

import fonda.scheduler.model.SchedulerConfig;
import fonda.scheduler.model.TaskConfig;
import io.fabric8.kubernetes.api.model.Node;
import io.fabric8.kubernetes.api.model.Pod;
import io.fabric8.kubernetes.client.KubernetesClient;
import io.fabric8.kubernetes.client.Watcher;
import io.fabric8.kubernetes.client.dsl.ExecListener;
import io.fabric8.kubernetes.client.dsl.ExecWatch;
import io.fabric8.kubernetes.client.dsl.PodResource;
import lombok.Getter;
import lombok.extern.slf4j.Slf4j;
import okhttp3.Response;

import java.io.*;
import java.nio.file.Path;
import java.util.*;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

@Slf4j
public class RandomScheduler extends  Scheduler {

    //For testing
    private final boolean local = true;
    private final String name;
    private final Map<String, String> daemonByNode = new HashMap<>();
    private final List<SchedulerConfig.LocalClaim> localClaims;
    private final Map<String, String> workdirNode = new HashMap<>();
    private final SchedulerConfig schedulerConfig;

    public RandomScheduler( String name, KubernetesClient client, SchedulerConfig config ) {
        super( name, client);
        this.name = name;
        this.localClaims = config.localClaims;
        this.schedulerConfig = config;
        log.info ( "Create scheduler: " + name );
    }

    public void schedule(Pod podToSchedule) {
        log.info ( "===========" );
        if( podToSchedule != null ){
            List<Node> items = getNodeList().getItems();
            int trial = 0;
            while( trial++ < 5 ) {
                if(isClose()) return;
                try {
                    Node node = items.get(new Random().nextInt(items.size()));
                    if( !daemonByNode.containsKey( node.getMetadata().getName() )) continue;

                    String workingDir = getWorkingDir(podToSchedule);
                    workdirNode.put(workingDir, node.getMetadata().getName());
                    //Copy files to this node
                    if( !copyConfig(workingDir, node) ){
                        log.info( "No success to copy config for " + workingDir + " to Node " + node.getMetadata().getName() );
                        continue;
                    }

                    TaskConfig config = getConfigFor(podToSchedule.getMetadata().getName());
                    if( !copyInputsToNode(config, node) ){
                        log.info( "No success to copy input for " + workingDir + " to Node " + node.getMetadata().getName() );
                        continue;
                    }
                    //Store Map: Workdir Node
                    assignPodToNode(podToSchedule, node);
                    break;
                }catch (Exception e){
                    log.info( "Try " + podToSchedule.getMetadata().getName() + " again on a different node..." );
                    e.printStackTrace();
                }
            }
            if(trial > 5 ){
                log.info( "Failed to schedule " + podToSchedule.getMetadata().getName() + " " + daemonByNode );
                podToSchedule.getStatus().setPhase("Failed");
            }
        } else {
            log.info ( "Pod was null." );
        }
        log.info ( "===========" );
    }

    void podEventReceived(Watcher.Action action, Pod pod){
        //TODO get current ExecutorPod else use local
        if( ( "mount-" + name.replace('_', '-') + "-" ).equals(pod.getMetadata().getGenerateName()) ){
            String nodeName = pod.getSpec().getNodeName();
            if ( nodeName != null ){
                if( action == Watcher.Action.DELETED ){
                    if ( pod.getMetadata().getName().equals( daemonByNode.get(nodeName) )){
                        daemonByNode.remove( nodeName );
                    }
                } else if (pod.getStatus().getPhase().equals("Running")) {
                    daemonByNode.put(nodeName, pod.getMetadata().getName() );
                } else if (pod.getStatus().getPhase().equals("Failed")) {
                    daemonByNode.remove( nodeName );
                } else {
                    daemonByNode.remove( nodeName );
                }
            }
        }
    }

    private boolean copyConfig( String workdir, Node node ){
        String destinationPod = daemonByNode.get( node.getMetadata().getName() );
        log.info ("Copy workdir: " + workdir + " to node: " + node.getMetadata().getName() + " (" + destinationPod + ") " + daemonByNode);
        if( local ){
            //SchedulerConfig.LocalClaim localClaim = localClaims.stream().filter(x -> workdir.startsWith(x.mountPath)).findFirst().get();
            String[] files = {".command.run", ".command.sh"};
            for( String file : files) {
                boolean success = false;
                int failureCount = 0;
                while( !success ) {
                    try{
                        Path workdirPath = new File(workdir + File.separator + file).toPath();
                        Boolean upload = findPodByName(destinationPod)
                                .file(workdir + File.separator + file)
                                .upload(workdirPath);
                        success = upload;
                        if(!success) log.info("No success uploading: " + file );
                    }catch(Exception e){
                        log.info ("Error for pod " + destinationPod + " on node: " + node.getMetadata().getName() );
                        if ( ++failureCount > 10 ) {
                            e.printStackTrace();
                            return false;
                        }
                        destinationPod = daemonByNode.get( node.getMetadata().getName() );
                        log.info ("Try again for pod " + destinationPod + " on node: " + node.getMetadata().getName() );
                    }

                }
            }
            return true;
        } else {
            throw new RuntimeException( "Currently this can not run in Kubernetes!" );
        }
    }

    @Override
    void onPodTermination( Pod pod ) {
        if(isClose()) return;
        //Copy results from Executor
        PodResource<Pod> daemon = findPodByName(daemonByNode.get(pod.getSpec().getNodeName()));
        String workingDir = getWorkingDir(pod);
        String[] files = {
                ".command.sh",
                ".command.out",
                ".exitcode",
                //".exitcode2",
                ".command.trace",
                ".command.infiles",
                ".command.outfiles",
                ".command.outfilesTmp"
        };
        if( local ){
            log.info("Start copying all files for " + pod.getSpec().getNodeName() );
            for( String file : files) {
                Path workdirPath = new File( workingDir + File.separator + file ).toPath();
                Boolean copy = daemon
                        .file(workingDir + File.separator + file)
                        .copy(workdirPath);
                if(!copy){
                    log.info("No success downloading: " + file );
                }
            }

            //For debugging: download logs
            try {
                final PrintWriter printWriter = new PrintWriter(workingDir + File.separator + ".command.kubelogs");
                printWriter.print(daemon.getLog());
                printWriter.close();
            } catch (FileNotFoundException e) {
                e.printStackTrace();
            }

            log.info("Copied all files for " + pod.getSpec().getNodeName() );
            try {
                new File( workingDir + File.separator + ".command.done" ).createNewFile();
            } catch (IOException e) {
                e.printStackTrace();
            }
        } else {
            throw new RuntimeException( "Currently this can not run in Kubernetes!" );
        }
    }

    private String extractWorkspace( String path ){
        if( path.startsWith( schedulerConfig.workDir ) ){
            return path.substring( 0, schedulerConfig.workDir.length() + 34 );
        }
        return null;
    }

    private boolean copyInputsToNode(TaskConfig config , Node node ){
        //TODO schedule copy
        //Learn copy Speed
        //Remember Filesize
        //Time = Size * Speed, order desc, run in this order, x parallel

        Map< String, List< String > > filesByNode = new HashMap<>();

        //Structure input files by node
        config.getInputs().forEach( x -> {
            log.info(x.toString());
            if( x.getValue() instanceof List ){
                for( Object o : (List) x.getValue()){
                    if( o instanceof Map ){
                        Map<String, Object> input = (Map<String, Object>) o;
                        if(input.containsKey( "sourceObj" )){
                            String sourceObj = (String) input.get( "sourceObj" );
                            String workspace = extractWorkspace(sourceObj);

                            if( workspace != null ){

                                final String podName = daemonByNode.get(workdirNode.get(workspace));
                                List<String> inputs;
                                if( !filesByNode.containsKey( podName ) ){
                                    inputs = new LinkedList<>();
                                    filesByNode.put( podName, inputs );
                                } else {
                                    inputs = filesByNode.get( podName );
                                }

                                inputs.add( sourceObj );

                            }
                        }
                    }
                }
            }
        } );

        return filesByNode.entrySet().stream().allMatch( x -> {
            return copyFile( x.getKey(), node.getMetadata().getName(), x.getValue() );
        });
        //Copy for x Nodes in parallel

    }

    private boolean copyFile(String fromPod, String toNode, List<String> files){

        String toPod = daemonByNode.get(toNode);
        if( toPod.equals(fromPod) ){
            return true;
        }

        //TODO reduce mkdir to the only necessary, remove files, if it already exists
        String[] command = new String[ files.size() * 9 - 1 ];
        int i = 0;
        for ( String file : files ) {
            command[i++] = "mkdir";
            command[i++] = "-p";
            command[i++] = file.substring(0, file.lastIndexOf("/")) ;
            command[i++] = "&&";
            command[i++] = "kubectl";
            command[i++] = "cp";
            command[i++] = fromPod + ":\"" + file + "\"";
            //if( file.endsWith( "/" ) ){
                command[i++] = "\"" + file + "\"";
            //} else {
            //    command[i++] = "\"" + file.substring( 0, file.lastIndexOf('/' ) ) + "\"";
            //}

            if( i < command.length) command[i++] = "&&";
        }
        //String command = file.stream()
                //.map(x -> "mkdir -p \"" + x.substring(0, x.lastIndexOf("/")) + "\" && kubectl cp " + fromPod + ":\"" + x + "\" \"" + x + "\"")
                //.collect(Collectors.joining(" && "));

        log.info( toNode + ": " + Arrays.toString(command) );


        final PodResource<Pod> pod = findPodByName(toPod);

        ByteArrayOutputStream out = new ByteArrayOutputStream();
        ByteArrayOutputStream error = new ByteArrayOutputStream();
        CountDownLatch execLatch = new CountDownLatch(1);

        final MyPodExecListener listener = new MyPodExecListener(execLatch);
        ExecWatch exec = pod
                .writingOutput(out)
                .writingError(error)
                .usingListener(listener)
                .exec("bash", "-c", String.join(" ", command ));
        try {
            execLatch.await(1, TimeUnit.DAYS );
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
        exec.close();
        log.info("Exec Output: {} ", out);
        if(listener.isFailure()){
            log.info ("Could not copy files from " + fromPod + " to " + toNode);
        }
        return !listener.isFailure();
    }

    private class MyPodExecListener implements ExecListener {

        private final CountDownLatch execLatch;

        @Getter
        private boolean failure = false;

        private MyPodExecListener( final CountDownLatch execLatch ){
            this.execLatch = execLatch;
        }

        @Override
        public void onOpen(Response response) {
            log.info("Shell was opened");
        }

        @Override
        public void onFailure(Throwable throwable, Response response) {
            log.info("Some error encountered");
            execLatch.countDown();
            failure = true;
        }

        @Override
        public void onClose(int i, String s) {
            log.info("Shell Closing");
            execLatch.countDown();
        }
    }

}
