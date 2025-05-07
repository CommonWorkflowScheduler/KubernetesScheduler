package cws.k8s.scheduler.util;

import cws.k8s.scheduler.model.NodeWithAlloc;
import cws.k8s.scheduler.model.Task;
import lombok.RequiredArgsConstructor;
import lombok.Setter;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.*;

public class TraceObserver {

    private final Map<Task, TraceItem> trace = new HashMap<>();

    public void addTask( Task task ) {
        final TraceItem traceItem = new TraceItem( task );
        synchronized ( trace ) {
            trace.put( task, traceItem );
        }
    }

    public void addCopyTask( Task task, NodeWithAlloc node, int phase ) {
        trace.get( task ).addCopyTask( node.getName(), phase );
    }

    public void setLabelsOnNode( Task task, List<String> labels ) {
        trace.get( task ).setLabelsOnNode( labels );
    }

    public void addWasPreparedOnNode( Task task, List<NodeWithAlloc> node ) {
        for ( NodeWithAlloc n : node ) {
            trace.get( task ).addWasPreparedOnNode( n.getName() );
        }
    }

    public void incOfferedForAdditionalCopy( Task task ){
        trace.get( task ).incOfferedForAdditionalCopy();
    }

    public void writeToFile( String filename ) {
        final List<String> lines = new LinkedList<>();
        lines.add( "podname;taskname;labels;node;labelsOnNode;copyTaskPhase2;copyTaskPhase3;wasPreparedOnNodes;outputsCopiedToNodes;offeredForAdditionalCopy" );
        trace.values().stream()
                .sorted( Comparator.comparingInt( x -> x.task.getId() ) )
                .forEach( traceItem -> lines.add( traceItem.toString() ) );
        try {
            Files.write( Paths.get( filename ), lines );
        } catch ( IOException e ) {
            e.printStackTrace();
        }
    }

    public void close(){
        final String pathname = "/input/data/scheduler/";
        writeToFile(pathname + "schedulertrace.csv" );
    }


    @RequiredArgsConstructor
    private static class TraceItem {
        private final Task task;
        private final List<String> copyTaskPhase2 = new LinkedList<>();
        private final List<String> copyTaskPhase3 = new LinkedList<>();
        private final Set<String> wasPreparedOnNodes = new HashSet<>();
        private final List<String> outputsCopiedToNodes = new LinkedList<>();
        @Setter
        private List<String> labelsOnNode;
        private int offeredForAdditionalCopy = 0;

        public void addCopyTask( String node, int phase ) {
            if ( phase == 2 ) {
                synchronized ( copyTaskPhase2 ) {
                    copyTaskPhase2.add( node );
                }
            } else if ( phase == 3 ) {
                synchronized ( copyTaskPhase3 ) {
                    copyTaskPhase3.add( node );
                }
            }
        }

        public void addWasPreparedOnNode( String node ) {
            synchronized ( wasPreparedOnNodes ) {
                wasPreparedOnNodes.add( node );
            }
        }

        public void addOutputCopiedToNode( String node ) {
            synchronized ( outputsCopiedToNodes ) {
                outputsCopiedToNodes.add( node );
            }
        }

        private void incOfferedForAdditionalCopy(){
            offeredForAdditionalCopy++;
        }

        public String toString() {
            return String.format( "%s;%s;%s;%s;%s;%s;%s;%s;%d",
                    task.getConfig().getRunName(),
                    task.getConfig().getName(),
                    task.getNode().getName(),
                    labelsOnNode,
                    copyTaskPhase2,
                    copyTaskPhase3,
                    wasPreparedOnNodes,
                    outputsCopiedToNodes,
                    offeredForAdditionalCopy
            );
        }
    }

}
