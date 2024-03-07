package cws.k8s.scheduler.prediction.extractor;

import cws.k8s.scheduler.model.Task;

public class MemoryExtractor implements VariableExtractor {

    @Override
    public double extractVariable( Task task ) {
        return task.getTaskMetrics().getPeakRss();
    }

}
