package cws.k8s.scheduler.scheduler.nodeassign;

import cws.k8s.scheduler.model.NodeWithAlloc;
import cws.k8s.scheduler.model.PodWithAge;
import cws.k8s.scheduler.model.Requirements;
import cws.k8s.scheduler.model.Task;
import cws.k8s.scheduler.model.tracing.TraceRecord;
import cws.k8s.scheduler.util.NodeTaskAlignment;
import lombok.extern.slf4j.Slf4j;

import java.math.BigDecimal;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;

@Slf4j
public class FairAssign extends NodeAssign {

    @Override
    public List<NodeTaskAlignment> getTaskNodeAlignment( List<Task> unscheduledTasks, Map<NodeWithAlloc, Requirements> availableByNode ) {

        LinkedList<NodeTaskAlignment> alignment = new LinkedList<>();
        for ( final Task task : unscheduledTasks ) {
            final PodWithAge pod = task.getPod();
            log.info("Pod: " + pod.getName() + " Requested Resources: " + pod.getRequest() );
            NodeWithAlloc bestNode = null;
            Double bestScore = null;
            final List<Double> costs = new LinkedList<>();
            final BigDecimal podRequest = pod.getRequest().getCpu();
            int triedOnNodes = 0;
            for ( Map.Entry<NodeWithAlloc, Requirements> e : availableByNode.entrySet() ) {
                if ( scheduler.canSchedulePodOnNode( e.getValue(), pod, e.getKey() ) ) {
                    triedOnNodes++;
                    final BigDecimal maxValue = e.getKey().getMaxResources().getCpu();
                    //how much is available if we assign this pod
                    final BigDecimal newValue = e.getValue().getCpu().subtract( podRequest );
                    //larger values are better => more resources available
                    final double score = newValue.doubleValue() /  maxValue.doubleValue();
                    if ( bestScore == null || score > bestScore ) {
                        bestScore = score;
                        bestNode = e.getKey();
                    }
                    costs.add( score );
                }
            }
            if ( bestNode != null ) {
                final TraceRecord traceRecord = task.getTraceRecord();
                traceRecord.foundAlignment();
                traceRecord.setSchedulerNodesTried( triedOnNodes );
                traceRecord.setSchedulerBestCost( bestScore );
                traceRecord.setSchedulerNodesCost( costs );
                alignment.add( new NodeTaskAlignment( bestNode, task ) );
                availableByNode.get( bestNode ).subFromThis( pod.getRequest() );
                log.info( "--> " + bestNode.getName() );
            }
        }
        return alignment;
    }

}
