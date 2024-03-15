package cws.k8s.scheduler.prediction.predictor;

import cws.k8s.scheduler.prediction.Predictor;
import lombok.extern.slf4j.Slf4j;
import org.jetbrains.annotations.NotNull;
import org.junit.Test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;

@Slf4j
public class PolynomialPredictorTest {

    @Test
    public void testOneTask() {
        Predictor lp = getPolyPredictor(2);
        lp.addTask( new TestTask( 1d, 1d ) );
        assertNull( lp.queryPrediction( new TestTask( 4d, 4d ) ) );
    }

    @Test
    public void testTwoTasks() {
        Predictor lp = getPolyPredictor(2);
        lp.addTask( new TestTask( 1d, 1d ) );
        lp.addTask( new TestTask( 2d, 2d ) );
        assertNull( lp.queryPrediction( new TestTask( 4d, 4d ) ) );
    }

    @Test
    public void testThreeTasksLinear() {
        Predictor lp = getPolyPredictor(2);
        lp.addTask( new TestTask( 1d, 1d ) );
        lp.addTask( new TestTask( 2d, 2d ) );
        lp.addTask( new TestTask( 3d, 3d ) );
        assertEquals( 4d, lp.queryPrediction( new TestTask( 4d, 4d ) ), 0.0001 );
        assertEquals( 0d, lp.queryPrediction( new TestTask( 0d, 0d ) ), 0.0001 );
    }

    @Test
    public void testThreeTasksPoly() {
        Predictor lp = getPolyPredictor(2);
        lp.addTask( new TestTask( 1d, 1d ) );
        lp.addTask( new TestTask( 2d, 2d ) );
        lp.addTask( new TestTask( 3d, 9d ) );
        assertEquals( 1d, lp.queryPrediction( new TestTask( 1d, 0d ) ), 0.0001 );
        assertEquals( 2d, lp.queryPrediction( new TestTask( 2d, 0d ) ), 0.0001 );
        assertEquals( 9d, lp.queryPrediction( new TestTask( 3d, 0d ) ), 0.0001 );
    }

    @Test
    public void noData() {
        Predictor lp = getPolyPredictor(2);
        assertNull(lp.queryPrediction( new TestTask( 4d, 4d ) ));
    }

    @Test
    public void measure(){
        int iterations = 1000;
        Predictor lp1 = getPolyPredictor(2);
        TestTask[] tasks = new TestTask[iterations];
        for (int i = 0; i < iterations; i++) {
            tasks[i] = new TestTask( (double)i, i );
        }
        long start = System.currentTimeMillis();
        for ( TestTask task : tasks ) {
            lp1.addTask( task );
        }
        log.info("Time LP: {}", System.currentTimeMillis() - start);
    }

    @Test
    public void compareLinear(){
        int iterations = 100;
        Predictor lp = getPolyPredictor(2);
        TestTask[] tasks = new TestTask[iterations];
        for (int i = 0; i < iterations; i++) {
            tasks[i] = new TestTask( i, i + Math.random() - 0.5 );
        }
        int n = 0;
        for ( TestTask task : tasks ) {
            lp.addTask( task );
            Double prediction1 = lp.queryPrediction( task );
            if ( prediction1 == null ) {
                continue;
            }
            assertEquals( task.y, lp.queryPrediction( task ), 1 );
        }
    }

    @Test
    public void comparePoly(){
        int iterations = 1000;
        Predictor lp = getPolyPredictor(2);
        TestTask[] tasks = new TestTask[iterations];
        for (int i = 0; i < iterations; i++) {
            final double x = Math.random() * 1000;
            tasks[i] = new TestTask( x, getY(x) + Math.random() - 0.5 );
        }
        int n = 0;
        for ( TestTask task : tasks ) {
            lp.addTask( task );
            Double prediction1 = lp.queryPrediction( task );
            if ( prediction1 == null ) {
                continue;
            }
            assertEquals( task.y, lp.queryPrediction( task ), 1 );
        }
        for ( int x = 0; x < 1000; x++ ) {
            final TestTask task = new TestTask( x, 0d );
            assertEquals( getY( x ), lp.queryPrediction( task ), 1 );
        }
    }

    private double getY( double x ){
        return 5 + 1.5 * x + 2 * x * x;
    }

    @NotNull
    static Predictor getPolyPredictor( int polynomialDegree ) {
        return new PolynomialPredictor( t -> ((TestTask) t).x, t -> ((TestTask) t).y, polynomialDegree );
    }

}