package cws.k8s.scheduler.prediction.predictor;

import org.jetbrains.annotations.NotNull;
import org.junit.Test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;

public class LinearPredictorTest {

    @Test
    public void testOneTask() {
        LinearPredictor lp = getLinearPredictor();
        lp.addTask( new TestTask( 1, 1 ) );
        assertNull( lp.queryPrediction( new TestTask( 4, 4 ) ) );
    }

    @Test
    public void testTwoTasks() {
        LinearPredictor lp = getLinearPredictor();
        lp.addTask( new TestTask( 1, 1 ) );
        lp.addTask( new TestTask( 2, 2 ) );
        assertEquals( (Double) 4d, lp.queryPrediction( new TestTask( 4, 4 ) ) );
    }

    @Test
    public void testPredict() {
        LinearPredictor lp = getLinearPredictor();
        lp.addTask( new TestTask( 1, 1 ) );
        lp.addTask( new TestTask( 2, 2 ) );
        lp.addTask( new TestTask( 3, 3 ) );
        assertEquals( (Double) 4d, lp.queryPrediction( new TestTask( 4, 4 ) ) );
        assertEquals(  (Double) 0d, lp.queryPrediction( new TestTask( 0, 0 ) ) );
    }

    @Test
    public void noData() {
        LinearPredictor lp = getLinearPredictor();
        assertNull(lp.queryPrediction( new TestTask( 4, 4 ) ));
    }

    @NotNull
    private static LinearPredictor getLinearPredictor() {
        return new LinearPredictor( t -> ((TestTask) t).x, t -> ((TestTask) t).y );
    }

}