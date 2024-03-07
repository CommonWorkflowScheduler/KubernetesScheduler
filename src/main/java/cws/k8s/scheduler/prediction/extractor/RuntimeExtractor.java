package cws.k8s.scheduler.prediction.extractor;

import cws.k8s.scheduler.model.Task;

public class RuntimeExtractor implements VariableExtractor {

    @Override
    public double extractVariable( Task task ) {
        return task.getTaskMetrics().getRealtime();
    }

}
