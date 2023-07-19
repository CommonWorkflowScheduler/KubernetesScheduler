package cws.k8s.scheduler.util;

import java.util.Collection;
import java.util.Collections;
import java.util.LinkedList;

public class SortedList<T extends Comparable<T>> extends LinkedList<T> {

    public SortedList( Collection<T> collection ) {
        super( collection );
        this.sort( Comparable::compareTo );
    }

    @Override
    public boolean add( T elem ) {
        int insertionIndex = Collections.binarySearch(this, elem );
        if (insertionIndex < 0) {
            insertionIndex = -(insertionIndex + 1);
        }
        super.add( insertionIndex, elem );
        return true;
    }

    @Override
    public void add( int index, T element ) {
        throw new UnsupportedOperationException( "Not supported" );
    }

    @Override
    public boolean addAll( Collection<? extends T> c ) {
        for ( T t : c ) {
            this.add( t );
        }
        return true;
    }

    @Override
    public boolean addAll( int index, Collection<? extends T> c ) {
        throw new UnsupportedOperationException( "Not supported" );
    }

    @Override
    public void addFirst( T t ) {
        throw new UnsupportedOperationException( "Not supported" );
    }

    @Override
    public void addLast( T t ) {
        throw new UnsupportedOperationException( "Not supported" );
    }
}
