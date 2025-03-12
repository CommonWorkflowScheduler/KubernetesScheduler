package cws.k8s.scheduler.prediction.offset;

import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.*;

class VarianceOffsetTest {

    @Test
    void calculateVariance() {
        final VarianceOffset varianceOffset = new VarianceOffset( null );
        double[] values = { 1, 1, 3, 2 };
        assertEquals( 0.91666667, varianceOffset.calculateVariance( values, 4 ), 0.00001 );

        values = new double[]{ 1, 1, 3, 1 };
        assertEquals( 1, varianceOffset.calculateVariance( values, 4 ), 0.00001 );

        assertEquals( 1 + 1/3.0, varianceOffset.calculateVariance( values, 3 ), 0.00001 );
    }
}