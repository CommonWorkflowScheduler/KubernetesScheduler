package cws.k8s.scheduler.publishDir;

import cws.k8s.scheduler.model.location.NodeLocation;
import lombok.RequiredArgsConstructor;

@RequiredArgsConstructor
public class InformPublishFinishedRunnable implements Runnable {

    private final PublishExecHolder execHolder;
    private final NodeLocation nodeLocation;


    @Override
    public void run() {
        execHolder.finishedOnNode( nodeLocation );
    }
}
