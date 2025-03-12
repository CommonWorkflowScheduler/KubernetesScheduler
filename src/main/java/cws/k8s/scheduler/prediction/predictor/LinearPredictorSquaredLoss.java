package cws.k8s.scheduler.prediction.predictor;

import cws.k8s.scheduler.model.Task;
import cws.k8s.scheduler.prediction.Predictor;
import cws.k8s.scheduler.prediction.extractor.VariableExtractor;
import lombok.RequiredArgsConstructor;
import org.apache.commons.math3.stat.regression.SimpleRegression;

import java.util.concurrent.atomic.AtomicLong;

@RequiredArgsConstructor
public class LinearPredictorSquaredLoss implements LinearPredictor {

    private final AtomicLong version = new AtomicLong( 0 );
    private final VariableExtractor inputExtractor;
    private final VariableExtractor outputExtractor;
    private final SimpleRegression regression = new SimpleRegression();


    @Override
    public void addTask( Task t ) {
        if ( t == null ) {
            throw new IllegalArgumentException( "Task cannot be null" );
        }
        double input = inputExtractor.extractVariable( t );
        double output = outputExtractor.extractVariable( t );
        synchronized ( regression ) {
            version.incrementAndGet();
            regression.addData( input, output );
        }
    }

    @Override
    public Double queryPrediction( Task task ) {
        final double predict;
        synchronized ( regression ) {
            predict = regression.predict( inputExtractor.extractVariable( task ) );
        }
        return Double.isNaN( predict ) ? null : predict;
    }

    @Override
    public double getDependentValue( Task task ) {
        return outputExtractor.extractVariable( task );
    }

    @Override
    public double getIndependentValue( Task task ) {
        return inputExtractor.extractVariable( task );
    }

    @Override
    public long getVersion() {
        return version.get();
    }

    public double getR() {
        synchronized ( regression ) {
            return regression.getR();
        }
    }

}
