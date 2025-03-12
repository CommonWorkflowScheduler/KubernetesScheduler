package cws.k8s.scheduler.prediction.predictor;

import org.jetbrains.annotations.NotNull;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

class LinearPredictorSquaredLossTest {

    @Test
    void testOneTask() {
        LinearPredictorSquaredLoss lp = getLinearPredictor();
        lp.addTask( new TestTask( 1d, 1d ) );
        Assertions.assertNull( lp.queryPrediction( new TestTask( 4d, 4d ) ) );
    }

    @Test
    void testTwoTasks() {
        LinearPredictorSquaredLoss lp = getLinearPredictor();
        lp.addTask( new TestTask( 1d, 1d ) );
        lp.addTask( new TestTask( 2d, 2d ) );
        Assertions.assertEquals( (Double) 4d, lp.queryPrediction( new TestTask( 4d, 4d ) ) );
    }

    @Test
    void testPredict() {
        LinearPredictorSquaredLoss lp = getLinearPredictor();
        lp.addTask( new TestTask( 1d, 1d ) );
        lp.addTask( new TestTask( 2d, 2d ) );
        lp.addTask( new TestTask( 3d, 3d ) );
        Assertions.assertEquals( (Double) 4d, lp.queryPrediction( new TestTask( 4d, 4d ) ) );
        Assertions.assertEquals( (Double) 0d, lp.queryPrediction( new TestTask( 0d, 0d ) ) );
    }

    @Test
    void noData() {
        LinearPredictorSquaredLoss lp = getLinearPredictor();
        Assertions.assertNull( lp.queryPrediction( new TestTask( 4d, 4d ) ) );
    }

    @NotNull
    private static LinearPredictorSquaredLoss getLinearPredictor() {
        return new LinearPredictorSquaredLoss( t -> ((TestTask) t).x, t -> ((TestTask) t).y );
    }

}