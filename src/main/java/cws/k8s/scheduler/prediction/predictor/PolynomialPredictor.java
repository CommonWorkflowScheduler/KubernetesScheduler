package cws.k8s.scheduler.prediction.predictor;

import cws.k8s.scheduler.model.Task;
import cws.k8s.scheduler.prediction.Predictor;
import cws.k8s.scheduler.prediction.extractor.VariableExtractor;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.math3.exception.NoDataException;
import org.apache.commons.math3.exception.NullArgumentException;
import org.apache.commons.math3.exception.util.LocalizedFormats;
import org.apache.commons.math3.stat.regression.OLSMultipleLinearRegression;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.AtomicLong;

@RequiredArgsConstructor
@Slf4j
public class PolynomialPredictor implements Predictor {

    private final AtomicLong version = new AtomicLong( 0 );
    private final VariableExtractor inputExtractor;
    private final VariableExtractor outputExtractor;
    private final OLSMultipleLinearRegression fitter = new OLSMultipleLinearRegression();
    private final List<double[]> observationsX = new ArrayList<>();
    private final List<Double> observationsY = new ArrayList<>();
    private final int polynomialDegree;
    double[] fitted;

    @Override
    public void addTask( Task t ) {
        if ( t == null ) {
            throw new IllegalArgumentException( "Task cannot be null" );
        }
        double input = inputExtractor.extractVariable( t );
        double output = outputExtractor.extractVariable( t );
        synchronized ( fitter ) {
            addPoint( input, output );
            if ( observationsX.size() <= polynomialDegree ) {
                return;
            }
            version.incrementAndGet();
            fitter.newSampleData( constructY(), constructX() );
            fitted = fitter.estimateRegressionParameters();
        }
    }

    private void addPoint( double x, double y ) {
        double[] xNew = new double[polynomialDegree];
        xNew[0] = x;
        for ( int j = 1; j < polynomialDegree; j++ ) {
            xNew[j] = Math.pow( x, j + 1d );
        }
        observationsX.add( xNew );
        observationsY.add( y );
    }

    private double[][] constructX(){
        double[][] x = new double[observationsX.size()][];
        int i = 0;
        for ( double[] doubles : observationsX ) {
            x[i++] = doubles;
        }
        return x;
    }

    private double[] constructY(){
        double[] y = new double[observationsY.size()];
        int i = 0;
        for ( Double aDouble : observationsY ) {
            y[i++] = aDouble;
        }
        return y;
    }

    @Override
    public Double queryPrediction( Task task ) {
        if ( fitted == null ) {
            return null;
        }
        return evaluate( fitted, inputExtractor.extractVariable( task ) );
    }

    @Override
    public double getDependentValue( Task task ) {
        return outputExtractor.extractVariable( task );
    }

    @Override
    public long getVersion() {
        return version.get();
    }

    private double evaluate(double[] coefficients, double argument)
            throws NullArgumentException, NoDataException {
        int n = coefficients.length;
        if (n == 0) {
            throw new NoDataException( LocalizedFormats.EMPTY_POLYNOMIALS_COEFFICIENTS_ARRAY);
        }
        double result = coefficients[n - 1];
        for (int j = n - 2; j >= 0; j--) {
            result = argument * result + coefficients[j];
        }
        return result;
    }

}
