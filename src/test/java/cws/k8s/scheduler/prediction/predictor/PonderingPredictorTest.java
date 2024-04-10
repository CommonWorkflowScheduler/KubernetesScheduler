package cws.k8s.scheduler.prediction.predictor;

import cws.k8s.scheduler.prediction.Predictor;
import org.jetbrains.annotations.NotNull;
import org.junit.Test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;

public class PonderingPredictorTest {

    @Test
    public void testOneTask() {
        Predictor lp = getPredictor();
        lp.addTask( new TestTask( 1d, 1d ) );
        assertEquals( (Double) 1d, lp.queryPrediction( new TestTask( 1d, 4d ) ) );
    }

    @Test
    public void testTwoTasks() {
        Predictor lp = getPredictor();
        lp.addTask( new TestTask( 1d, 1d ) );
        lp.addTask( new TestTask( 2d, 2d ) );
        assertEquals( (Double) 4d, lp.queryPrediction( new TestTask( 4d, 4d ) ) );
    }

    @Test
    public void testPredict() {
        Predictor lp = getPredictor();
        lp.addTask( new TestTask( 1d, 1d ) );
        lp.addTask( new TestTask( 2d, 2d ) );
        lp.addTask( new TestTask( 3d, 3d ) );
        assertEquals( (Double) 4d, lp.queryPrediction( new TestTask( 4d, 4d ) ) );
        assertEquals(  (Double) 0d, lp.queryPrediction( new TestTask( 0d, 0d ) ) );
    }

    @Test
    public void testPredictNoCorrelation() {
        Predictor lp = getPredictor();
        lp.addTask( new TestTask( 2d, 1d ) );
        lp.addTask( new TestTask( 3d, 3d ) );
        lp.addTask( new TestTask( 4d, 1d ) );
        lp.addTask( new TestTask( 4.1d, 2d ) );
        assertEquals( 7d / 4, lp.queryPrediction( new TestTask( 4d, 4d ) ), 0.0001 );
        assertEquals( 7d / 4, lp.queryPrediction( new TestTask( 0d, 0d ) ), 0.0001 );
    }

    @Test
    public void noData() {
        Predictor lp = getPredictor();
        assertNull(lp.queryPrediction( new TestTask( 4d, 4d ) ));
    }

    @NotNull
    private static Predictor getPredictor() {
        return new PonderingPredictor( new LinearPredictorSquaredLoss( t -> ((TestTask) t).x, t -> ((TestTask) t).y) );
    }

}