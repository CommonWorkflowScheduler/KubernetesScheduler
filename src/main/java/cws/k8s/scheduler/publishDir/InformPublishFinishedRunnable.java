package cws.k8s.scheduler.publishDir;

import cws.k8s.scheduler.model.location.NodeLocation;
import lombok.RequiredArgsConstructor;

import java.util.Map;

@RequiredArgsConstructor
public class InformPublishFinishedRunnable implements Runnable {

    private final PublishExecHolder execHolder;
    private final NodeLocation nodeLocation;
    private final Map<NodeLocation, Integer> currentPublishJobsPerNode;


    @Override
    public void run() {
        execHolder.finishedOnNode( nodeLocation );
        currentPublishJobsPerNode.compute( nodeLocation, ( k, v ) -> v == null ? 0 : v - 1 );
    }
}
