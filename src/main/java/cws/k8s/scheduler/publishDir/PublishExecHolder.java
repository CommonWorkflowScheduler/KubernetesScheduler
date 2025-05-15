package cws.k8s.scheduler.publishDir;

import cws.k8s.scheduler.model.location.NodeLocation;

import java.util.LinkedList;
import java.util.Map;
import java.util.Queue;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

public class PublishExecHolder {

    private final static int MAX_RUNNING = 1;
    private final Map<NodeLocation, Queue<Runnable>> runnables = new java.util.HashMap<>();
    private final Map<NodeLocation, Integer> runningOnNode = new java.util.HashMap<>();
    private final ExecutorService executor = Executors.newCachedThreadPool();

    public synchronized void addRunnable(NodeLocation node, Runnable runnable) {
        runnables.computeIfAbsent(node, k -> new LinkedList<>() ).add(runnable);
        start( node );
    }

    public synchronized void finishedOnNode( NodeLocation node ) {
        runningOnNode.compute( node, ( k, v ) -> v == null ? 0 : v - 1 );
        start( node );
    }

    private synchronized void start( NodeLocation node ) {
        if ( runnables.containsKey( node ) ) {
            Queue<Runnable> runnablesList = runnables.get( node );
            if ( runnablesList.isEmpty() || runningOnNode.getOrDefault( node, 0 ) >= MAX_RUNNING ) {
                return;
            }
            runningOnNode.compute( node, ( k, v ) -> v == null ? 1 : v + 1 );
            final Runnable runnable = runnablesList.poll();
            if ( runnable != null ) {
                executor.submit(runnable);
            }
        }
    }
}
