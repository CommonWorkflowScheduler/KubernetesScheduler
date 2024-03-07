package cws.k8s.scheduler.prediction.predictor;

import cws.k8s.scheduler.model.Task;
import cws.k8s.scheduler.prediction.Predictor;
import cws.k8s.scheduler.prediction.extractor.VariableExtractor;
import lombok.RequiredArgsConstructor;

@RequiredArgsConstructor
public class ConstantNumberPredictor implements Predictor {

    private final VariableExtractor outputExtractor;
    final private double constant;

    @Override
    public void addTask( Task t ) {}

    @Override
    public Double queryPrediction( Task task ) {
        return constant;
    }

    @Override
    public double getDependentValue( Task task ) {
        return outputExtractor.extractVariable( task );
    }
}
