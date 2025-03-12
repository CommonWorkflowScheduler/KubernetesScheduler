package cws.k8s.scheduler.prediction.extractor;

import cws.k8s.scheduler.model.Task;

public interface VariableExtractor {

    double extractVariable( Task task );

}
