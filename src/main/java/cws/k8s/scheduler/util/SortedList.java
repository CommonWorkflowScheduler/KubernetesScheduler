package cws.k8s.scheduler.util;

import java.util.Collection;
import java.util.LinkedList;
import java.util.ListIterator;

public class SortedList<T extends Comparable<T>> extends LinkedList<T> {

    public SortedList( Collection<T> collection ) {
        super( collection );
        this.sort( Comparable::compareTo );
    }

    @Override
    public boolean add( T elem ) {
        final ListIterator<T> iterator = this.listIterator();
        while ( iterator.hasNext() ) {
            final Comparable<T> next = iterator.next();
            if ( next.compareTo( elem ) > 0 ) {
                if ( iterator.hasPrevious() ) {
                    iterator.previous();
                    iterator.add( elem );
                } else {
                    //first element of list
                    super.addFirst( elem );
                }
                return true;
            }
        }
        iterator.add( elem );
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
