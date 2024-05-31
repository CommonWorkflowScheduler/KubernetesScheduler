package cws.k8s.scheduler.prediction.predictor;

import cws.k8s.scheduler.model.Task;
import cws.k8s.scheduler.prediction.Predictor;
import cws.k8s.scheduler.prediction.extractor.VariableExtractor;
import cws.k8s.scheduler.prediction.predictor.loss.UnequalLossFunction;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.math3.optim.InitialGuess;
import org.apache.commons.math3.optim.MaxEval;
import org.apache.commons.math3.optim.nonlinear.scalar.GoalType;
import org.apache.commons.math3.optim.nonlinear.scalar.ObjectiveFunction;
import org.apache.commons.math3.optim.nonlinear.scalar.noderiv.NelderMeadSimplex;
import org.apache.commons.math3.optim.nonlinear.scalar.noderiv.SimplexOptimizer;
import org.apache.commons.math3.stat.regression.SimpleRegression;

import java.util.concurrent.atomic.AtomicLong;

@Slf4j
public class LinearPredictorCustomLoss implements LinearPredictor {

    private final AtomicLong version = new AtomicLong( 0 );
    private final VariableExtractor inputExtractor;
    private final VariableExtractor outputExtractor;
    private final UnequalLossFunction unequalLossFunction;
    private double[] optimizedParameters = null;
    private final SimpleRegression regression = new SimpleRegression();

    private static final int BYTES_IN_GB = 1024 * 1024 * 1024;


    public LinearPredictorCustomLoss( VariableExtractor inputExtractor, VariableExtractor outputExtractor, double weightOverprediction ) {
        this.inputExtractor = inputExtractor;
        this.outputExtractor = outputExtractor;
        unequalLossFunction = new UnequalLossFunction(weightOverprediction);
    }

    public LinearPredictorCustomLoss( VariableExtractor inputExtractor, VariableExtractor outputExtractor ) {
        this( inputExtractor, outputExtractor, 0.02 );
    }


    @Override
    public void addTask( Task t ) {
        if ( t == null ) {
            throw new IllegalArgumentException( "Task cannot be null" );
        }
        double input = inputExtractor.extractVariable( t ) / BYTES_IN_GB;
        double output = outputExtractor.extractVariable( t ) / BYTES_IN_GB;
        synchronized ( unequalLossFunction ) {
            version.incrementAndGet();
            unequalLossFunction.addPoint( input, output );
            optimizedParameters = null;
            regression.addData( input, output );
        }
    }

    @Override
    public Double queryPrediction( Task task ) {
        synchronized ( unequalLossFunction ) {
            if ( !unequalLossFunction.canTrain() ) {
                return null;
            }
            if ( optimizedParameters == null ) {
                SimplexOptimizer optimizer = new SimplexOptimizer(1e-30, 1e-50);
                double[] startPoint = { regression.getIntercept(), regression.getSlope() }; // Initial guess for the parameters
                NelderMeadSimplex simplex = new NelderMeadSimplex(startPoint.length);
                optimizedParameters = optimizer.optimize(
                        MaxEval.unlimited(),
                        new ObjectiveFunction(unequalLossFunction),
                        GoalType.MINIMIZE,
                        new InitialGuess(startPoint),
                        simplex
                ).getPoint();
            }
            final double x = inputExtractor.extractVariable( task ) / BYTES_IN_GB;
            final double prediction = optimizedParameters[0] + x * optimizedParameters[1];
            return prediction * BYTES_IN_GB;
        }
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
