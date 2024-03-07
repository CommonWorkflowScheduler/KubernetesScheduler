package cws.k8s.scheduler.prediction.predictor;

import cws.k8s.scheduler.model.Task;
import cws.k8s.scheduler.prediction.Predictor;
import cws.k8s.scheduler.prediction.extractor.VariableExtractor;
import lombok.RequiredArgsConstructor;
import org.apache.commons.math3.stat.regression.SimpleRegression;

@RequiredArgsConstructor
public class LinearPredictor implements Predictor {

    private final VariableExtractor inputExtractor;
    private final VariableExtractor outputExtractor;
    private final SimpleRegression regression = new SimpleRegression();


    @Override
    public void addTask( Task t ) {
        double input = inputExtractor.extractVariable( t );
        double output = outputExtractor.extractVariable( t );
        regression.addData( input, output );
    }

    @Override
    public Double queryPrediction( Task task ) {
        final double predict = regression.predict( inputExtractor.extractVariable( task ) );
        return Double.isNaN( predict ) ? null : predict;
    }

    @Override
    public double getDependentValue( Task task ) {
        return outputExtractor.extractVariable( task );
    }
}
