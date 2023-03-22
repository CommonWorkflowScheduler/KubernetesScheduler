package cws.k8s.scheduler.util;

import lombok.extern.slf4j.Slf4j;
import org.junit.jupiter.api.Test;

import java.util.Arrays;
import java.util.LinkedList;
import java.util.List;

import static org.junit.jupiter.api.Assertions.*;

@Slf4j
class SortedListTest {

    @Test
    void constructorTest() {
        final List<Integer> integers = Arrays.asList( 8, 4, 1, 5 );
        final SortedList<Integer> sortedList = new SortedList<>( integers );
        assertEquals( 1, sortedList.get( 0 ) );
        assertEquals( 4, sortedList.get( 1 ) );
        assertEquals( 5, sortedList.get( 2 ) );
        assertEquals( 8, sortedList.get( 3 ) );
    }

    @Test
    void add() {
        final SortedList<Integer> sortedList = new SortedList<>( Arrays.asList( 9, 0, 4, 7 ) );
        log.info( sortedList.toString() );
        sortedList.add( 8 );
        log.info( sortedList.toString() );
        assertEquals( 8, sortedList.get( 3 ) );
        sortedList.add( 3 );
        log.info( sortedList.toString() );
        assertEquals( 3, sortedList.get( 1 ) );
        sortedList.add( 1 );
        log.info( sortedList.toString() );
        assertEquals( 1, sortedList.get( 1 ) );
        sortedList.add( 5 );
        log.info( sortedList.toString() );
        assertEquals( 0, sortedList.get( 0 ) );
        assertEquals( 1, sortedList.get( 1 ) );
        assertEquals( 3, sortedList.get( 2 ) );
        assertEquals( 4, sortedList.get( 3 ) );
        assertEquals( 5, sortedList.get( 4 ) );
        assertEquals( 7, sortedList.get( 5 ) );
        assertEquals( 8, sortedList.get( 6 ) );
        assertEquals( 9, sortedList.get( 7 ) );

    }

    @Test
    void addWithDuplicates() {
        final SortedList<Integer> sortedList = new SortedList<>( Arrays.asList( 9, 0, 4, 7 ) );
        sortedList.add( 8 );
        assertEquals( 8, sortedList.get( 3 ) );
        sortedList.add( 4 );
        assertEquals( 4, sortedList.get( 1 ) );
        assertEquals( 4, sortedList.get( 2 ) );
        sortedList.add( 1 );
        assertEquals( 1, sortedList.get( 1 ) );
        sortedList.add( 5 );
        log.info( sortedList.toString() );
        assertEquals( 0, sortedList.get( 0 ) );
        assertEquals( 1, sortedList.get( 1 ) );
        assertEquals( 4, sortedList.get( 2 ) );
        assertEquals( 4, sortedList.get( 3 ) );
        assertEquals( 5, sortedList.get( 4 ) );
        assertEquals( 7, sortedList.get( 5 ) );
        assertEquals( 8, sortedList.get( 6 ) );
        assertEquals( 9, sortedList.get( 7 ) );

    }

    @Test
    void addToEmptyList() {
        final SortedList<Integer> sortedList = new SortedList<>( new LinkedList< Integer >() );
        sortedList.add( 4 );
        log.info( sortedList.toString() );
        assertEquals( 4, sortedList.get( 0 ) );
    }

    @Test
    void addAtEnd() {
        final SortedList<Integer> sortedList = new SortedList<>( Arrays.asList( 5 ) );
        sortedList.add( 6 );
        log.info( sortedList.toString() );
        assertEquals( 5, sortedList.get( 0 ) );
        assertEquals( 6, sortedList.get( 1 ) );
    }

    @Test
    void addAtBeginning() {
        final SortedList<Integer> sortedList = new SortedList<>( Arrays.asList( 5 ) );
        sortedList.add( 4 );
        log.info( sortedList.toString() );
        assertEquals( 4, sortedList.get( 0 ) );
        assertEquals( 5, sortedList.get( 1 ) );
    }


    @Test
    void addAtMiddle() {
        final SortedList<Integer> sortedList = new SortedList<>( Arrays.asList( 4, 6 ) );
        sortedList.add( 5 );
        log.info( sortedList.toString() );
        assertEquals( 4, sortedList.get( 0 ) );
        assertEquals( 5, sortedList.get( 1 ) );
        assertEquals( 6, sortedList.get( 2 ) );
    }

    @Test
    void addAll() {
        final SortedList<Integer> sortedList = new SortedList<>( Arrays.asList( 9, 0, 4, 7 ) );
        sortedList.addAll( Arrays.asList( 8, 3, 1, 5 ) );
        assertEquals( 0, sortedList.get( 0 ) );
        assertEquals( 1, sortedList.get( 1 ) );
        assertEquals( 3, sortedList.get( 2 ) );
        assertEquals( 4, sortedList.get( 3 ) );
        assertEquals( 5, sortedList.get( 4 ) );
        assertEquals( 7, sortedList.get( 5 ) );
        assertEquals( 8, sortedList.get( 6 ) );
        assertEquals( 9, sortedList.get( 7 ) );
    }

    @Test
    void addAllWithDuplicates() {
        final SortedList<Integer> sortedList = new SortedList<>( Arrays.asList( 9, 0, 4, 7 ) );
        sortedList.addAll( Arrays.asList( 8, 4, 1, 5 ) );
        assertEquals( 0, sortedList.get( 0 ) );
        assertEquals( 1, sortedList.get( 1 ) );
        assertEquals( 4, sortedList.get( 2 ) );
        assertEquals( 4, sortedList.get( 3 ) );
        assertEquals( 5, sortedList.get( 4 ) );
        assertEquals( 7, sortedList.get( 5 ) );
        assertEquals( 8, sortedList.get( 6 ) );
        assertEquals( 9, sortedList.get( 7 ) );
    }
}