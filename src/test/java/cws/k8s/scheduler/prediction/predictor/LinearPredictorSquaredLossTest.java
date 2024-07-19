package cws.k8s.scheduler.prediction.predictor;

import org.jetbrains.annotations.NotNull;
import org.junit.Test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;

public class LinearPredictorSquaredLossTest {

    @Test
    public void testOneTask() {
        LinearPredictorSquaredLoss lp = getLinearPredictor();
        lp.addTask( new TestTask( 1d, 1d ) );
        assertNull( lp.queryPrediction( new TestTask( 4d, 4d ) ) );
    }

    @Test
    public void testTwoTasks() {
        LinearPredictorSquaredLoss lp = getLinearPredictor();
        lp.addTask( new TestTask( 1d, 1d ) );
        lp.addTask( new TestTask( 2d, 2d ) );
        assertEquals( (Double) 4d, lp.queryPrediction( new TestTask( 4d, 4d ) ) );
    }

    @Test
    public void testPredict() {
        LinearPredictorSquaredLoss lp = getLinearPredictor();
        lp.addTask( new TestTask( 1d, 1d ) );
        lp.addTask( new TestTask( 2d, 2d ) );
        lp.addTask( new TestTask( 3d, 3d ) );
        assertEquals( (Double) 4d, lp.queryPrediction( new TestTask( 4d, 4d ) ) );
        assertEquals(  (Double) 0d, lp.queryPrediction( new TestTask( 0d, 0d ) ) );
    }

    @Test
    public void noData() {
        LinearPredictorSquaredLoss lp = getLinearPredictor();
        assertNull(lp.queryPrediction( new TestTask( 4d, 4d ) ));
    }

    @NotNull
    private static LinearPredictorSquaredLoss getLinearPredictor() {
        return new LinearPredictorSquaredLoss( t -> ((TestTask) t).x, t -> ((TestTask) t).y );
    }

}