package cws.k8s.scheduler.model;

import lombok.extern.slf4j.Slf4j;
import org.junit.jupiter.api.Test;

import java.math.BigDecimal;
import java.util.HashSet;

import static org.junit.jupiter.api.Assertions.*;

@Slf4j
class RequirementsTest {

    @Test
    void ZERO() {
        final Requirements zero = ImmutableRequirements.ZERO;
        assertEquals( zero, new Requirements( BigDecimal.ZERO, BigDecimal.ZERO ) );
        assertEquals( BigDecimal.ZERO, zero.getCpu() );
        assertEquals( BigDecimal.ZERO, zero.getRam() );
    }

    @Test
    void addToThis() {
        Requirements a = new Requirements( BigDecimal.valueOf( 5 ), BigDecimal.valueOf( 6 ) );
        Requirements b = new Requirements( BigDecimal.valueOf( 7 ), BigDecimal.valueOf( 8 ) );
        Requirements c = a.addToThis( b );
        assertSame( a, c );
        assertEquals( BigDecimal.valueOf( 12 ), c.getCpu() );
        assertEquals( BigDecimal.valueOf( 14 ), c.getRam() );
    }

    @Test
    void add() {
        Requirements a = new Requirements( BigDecimal.valueOf( 5 ), BigDecimal.valueOf( 6 ) );
        Requirements b = new Requirements( BigDecimal.valueOf( 7 ), BigDecimal.valueOf( 8 ) );
        Requirements c = a.add( b );
        assertNotSame( a, c );
        assertEquals( BigDecimal.valueOf( 12 ), c.getCpu() );
        assertEquals( BigDecimal.valueOf( 14 ), c.getRam() );
        assertEquals( BigDecimal.valueOf( 5 ), a.getCpu() );
        assertEquals( BigDecimal.valueOf( 6 ), a.getRam() );
    }

    @Test
    void addRAMtoThis() {
        Requirements a = new Requirements( BigDecimal.valueOf( 5 ), BigDecimal.valueOf( 6 ) );
        a.addRAMtoThis( BigDecimal.valueOf( 8 ) );
        assertEquals( BigDecimal.valueOf( 5 ), a.getCpu() );
        assertEquals( BigDecimal.valueOf( 14 ), a.getRam() );
    }

    @Test
    void addCPUtoThis() {
        Requirements a = new Requirements( BigDecimal.valueOf( 5 ), BigDecimal.valueOf( 6 ) );
        a.addCPUtoThis( BigDecimal.valueOf( 8 ) );
        assertEquals( BigDecimal.valueOf( 13 ), a.getCpu() );
        assertEquals( BigDecimal.valueOf( 6 ), a.getRam() );
    }

    @Test
    void subFromThis() {
        Requirements a = new Requirements( BigDecimal.valueOf( 5 ), BigDecimal.valueOf( 6 ) );
        Requirements b = new Requirements( BigDecimal.valueOf( 3 ), BigDecimal.valueOf( 5 ) );
        Requirements c = a.subFromThis( b );
        assertSame( a, c );
        assertEquals( BigDecimal.valueOf( 2 ), c.getCpu() );
        assertEquals( BigDecimal.valueOf( 1 ), c.getRam() );
    }

    @Test
    void sub() {
        Requirements a = new Requirements( BigDecimal.valueOf( 5 ), BigDecimal.valueOf( 6 ) );
        Requirements b = new Requirements( BigDecimal.valueOf( 3 ), BigDecimal.valueOf( 5 ) );
        Requirements c = a.sub( b );
        assertNotSame( a, c );
        assertEquals( BigDecimal.valueOf( 2 ), c.getCpu() );
        assertEquals( BigDecimal.valueOf( 1 ), c.getRam() );
        assertEquals( BigDecimal.valueOf( 5 ), a.getCpu() );
        assertEquals( BigDecimal.valueOf( 6 ), a.getRam() );
    }

    @Test
    void multiply() {
        Requirements a = new Requirements( BigDecimal.valueOf( 5 ), BigDecimal.valueOf( 6 ) );
        Requirements b = a.multiply( BigDecimal.valueOf( 3 ) );
        assertNotSame( a, b );
        assertEquals( BigDecimal.valueOf( 15 ) , b.getCpu() );
        assertEquals( BigDecimal.valueOf( 18 ) , b.getRam() );
        assertEquals( BigDecimal.valueOf( 5 ) , a.getCpu() );
        assertEquals( BigDecimal.valueOf( 6 ) , a.getRam() );
    }

    @Test
    void multiplyToThis() {
        Requirements a = new Requirements( BigDecimal.valueOf( 5 ), BigDecimal.valueOf( 6 ) );
        Requirements b = a.multiplyToThis( BigDecimal.valueOf( 3 ) );
        assertSame( a, b );
        assertEquals( BigDecimal.valueOf( 15 ), b.getCpu() );
        assertEquals( BigDecimal.valueOf( 18 ), b.getRam() );
    }

    @Test
    void testHigherOrEquals() {
        Requirements a = new Requirements( BigDecimal.valueOf( 5 ), BigDecimal.valueOf( 6 ) );
        Requirements b = new Requirements( BigDecimal.valueOf( 3 ), BigDecimal.valueOf( 5 ) );
        assertTrue( a.higherOrEquals( b ) );
        assertFalse( b.higherOrEquals( a ) );
        assertTrue( a.higherOrEquals( a ) );
        assertTrue( b.higherOrEquals( b ) );
    }

    @Test
    void testClone() {
        Requirements a = new Requirements( BigDecimal.valueOf( 5 ), BigDecimal.valueOf( 6 ) );
        Requirements b = a.clone();
        assertNotSame( a, b );
        assertEquals( a.getCpu(), b.getCpu() );
        assertEquals( a.getRam(), b.getRam() );
    }


    @Test
    void smaller() {
        Requirements a = new Requirements( BigDecimal.valueOf( 5 ), BigDecimal.valueOf( 6 ) );
        Requirements b = new Requirements( BigDecimal.valueOf( 3 ), BigDecimal.valueOf( 5 ) );
        assertFalse( a.smaller( b ) );
        assertTrue( b.smaller( a ) );
        assertFalse( a.smaller( a ) );
        assertFalse( b.smaller( b ) );

        Requirements c = new Requirements( BigDecimal.valueOf( 6 ), BigDecimal.valueOf( 5 ) );
        assertFalse( a.smaller( c ) );
        assertFalse( c.smaller( a ) );
        assertFalse( c.smaller( c ) );
    }

    @Test
    void smallerEquals() {
        Requirements a = new Requirements( BigDecimal.valueOf( 5 ), BigDecimal.valueOf( 6 ) );
        Requirements b = new Requirements( BigDecimal.valueOf( 3 ), BigDecimal.valueOf( 5 ) );
        assertFalse( a.smallerEquals( b ) );
        assertTrue( b.smallerEquals( a ) );
        assertTrue( a.smallerEquals( a ) );
        assertTrue( b.smallerEquals( b ) );

        Requirements c = new Requirements( BigDecimal.valueOf( 6 ), BigDecimal.valueOf( 5 ) );
        assertFalse( a.smallerEquals( c ) );
        assertFalse( c.smallerEquals( a ) );
        assertTrue( c.smallerEquals( c ) );

        Requirements d = new Requirements( BigDecimal.valueOf( 5 ), BigDecimal.valueOf( 6 ) );
        assertTrue( a.smallerEquals( d ) );
        assertTrue( d.smallerEquals( a ) );
        assertTrue( d.smallerEquals( d ) );
    }

    @Test
    void getCpu() {
        Requirements a = new Requirements( BigDecimal.valueOf( 5 ), BigDecimal.valueOf( 6 ) );
        assertEquals( BigDecimal.valueOf( 5 ), a.getCpu() );
    }

    @Test
    void getRam() {
        Requirements a = new Requirements( BigDecimal.valueOf( 5 ), BigDecimal.valueOf( 6 ) );
        assertEquals( BigDecimal.valueOf( 6 ), a.getRam() );
    }


    @Test
    void atLeastOneBigger() {
        Requirements a = new Requirements( BigDecimal.valueOf( 5 ), BigDecimal.valueOf( 6 ) );
        Requirements b = new Requirements( BigDecimal.valueOf( 3 ), BigDecimal.valueOf( 5 ) );
        assertTrue( a.atLeastOneBigger( b ) );
        assertFalse( b.atLeastOneBigger( a ) );
        assertFalse( a.atLeastOneBigger( a ) );
        assertFalse( b.atLeastOneBigger( b ) );

        Requirements c = new Requirements( BigDecimal.valueOf( 6 ), BigDecimal.valueOf( 5 ) );
        assertTrue( a.atLeastOneBigger( c ) );
        assertTrue( c.atLeastOneBigger( a ) );
        assertFalse( c.atLeastOneBigger( c ) );

        Requirements d = new Requirements( BigDecimal.valueOf( 5 ), BigDecimal.valueOf( 6 ) );
        assertFalse( a.atLeastOneBigger( d ) );
        assertFalse( d.atLeastOneBigger( a ) );
        assertFalse( d.atLeastOneBigger( d ) );
    }

    @Test
    void hashSetTest() {
        Requirements a = new Requirements( BigDecimal.valueOf( 5 ), BigDecimal.valueOf( 6 ) );
        Requirements b = new Requirements( BigDecimal.valueOf( 5 ), BigDecimal.valueOf( 6 ) );
        final HashSet<Requirements> requirements = new HashSet<>();
        requirements.add( a );
        requirements.add( b );
        assertEquals( 1, requirements.size() );
        requirements.add( a.add( b ) );
        assertEquals( 2, requirements.size() );
        requirements.add( a.add( b ).sub( new Requirements( BigDecimal.valueOf( 5 ), BigDecimal.valueOf( 6 ) ) ) );
        assertEquals( 2, requirements.size() );
    }

    @Test
    void equalsTest() {
        Requirements a = new Requirements( BigDecimal.valueOf( 5 ), BigDecimal.valueOf( 6 ) );
        Requirements b = new Requirements( BigDecimal.valueOf( 5 ), BigDecimal.valueOf( 6 ) );
        assertEquals( a, b );
        assertEquals( b, a );
        assertEquals( a, a );
        assertEquals( b, b );
        assertNotEquals( null, a );
        assertNotEquals( a, new Object() );
        assertNotEquals( a, new Requirements( BigDecimal.valueOf( 5 ), BigDecimal.valueOf( 7 ) ) );
        assertNotEquals( a, new Requirements( BigDecimal.valueOf( 6 ), BigDecimal.valueOf( 6 ) ) );
        assertNotSame( a, b );
    }


}