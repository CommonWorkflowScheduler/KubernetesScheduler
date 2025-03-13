package cws.k8s.scheduler.prediction.predictor;

import cws.k8s.scheduler.prediction.Predictor;
import lombok.extern.slf4j.Slf4j;
import org.jetbrains.annotations.NotNull;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

@Slf4j
class PolynomialPredictorTest {

    @Test
    void testOneTask() {
        Predictor lp = getPolyPredictor(2);
        lp.addTask( new TestTask( 1d, 1d ) );
        Assertions.assertNull( lp.queryPrediction( new TestTask( 4d, 4d ) ) );
    }

    @Test
    void testTwoTasks() {
        Predictor lp = getPolyPredictor(2);
        lp.addTask( new TestTask( 1d, 1d ) );
        lp.addTask( new TestTask( 2d, 2d ) );
        Assertions.assertNull( lp.queryPrediction( new TestTask( 4d, 4d ) ) );
    }

    @Test
    void testThreeTasksLinear() {
        Predictor lp = getPolyPredictor(2);
        lp.addTask( new TestTask( 1d, 1d ) );
        lp.addTask( new TestTask( 2d, 2d ) );
        lp.addTask( new TestTask( 3d, 3d ) );
        Assertions.assertEquals( 4d, lp.queryPrediction( new TestTask( 4d, 4d ) ), 0.0001 );
        Assertions.assertEquals( 0d, lp.queryPrediction( new TestTask( 0d, 0d ) ), 0.0001 );
    }

    @Test
    void testThreeTasksPoly() {
        Predictor lp = getPolyPredictor(2);
        lp.addTask( new TestTask( 1d, 1d ) );
        lp.addTask( new TestTask( 2d, 2d ) );
        lp.addTask( new TestTask( 3d, 9d ) );
        Assertions.assertEquals( 1d, lp.queryPrediction( new TestTask( 1d, 0d ) ), 0.0001 );
        Assertions.assertEquals( 2d, lp.queryPrediction( new TestTask( 2d, 0d ) ), 0.0001 );
        Assertions.assertEquals( 9d, lp.queryPrediction( new TestTask( 3d, 0d ) ), 0.0001 );
    }

    @Test
    void noData() {
        Predictor lp = getPolyPredictor(2);
        Assertions.assertNull( lp.queryPrediction( new TestTask( 4d, 4d ) ) );
    }

    @Test
    void measure(){
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
    void compareLinear(){
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
            Assertions.assertEquals( task.y, lp.queryPrediction( task ), 1 );
        }
    }

    @Test
    void comparePoly(){
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
            Assertions.assertEquals( task.y, lp.queryPrediction( task ), 1 );
        }
        for ( int x = 0; x < 1000; x++ ) {
            final TestTask task = new TestTask( x, 0d );
            Assertions.assertEquals( getY( x ), lp.queryPrediction( task ), 1 );
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