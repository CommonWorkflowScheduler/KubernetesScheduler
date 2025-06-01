package cws.k8s.scheduler.scheduler;

import lombok.RequiredArgsConstructor;

import java.util.LinkedList;
import java.util.Queue;
import java.util.function.Consumer;

@RequiredArgsConstructor
public class FinalizerThread<T> extends Thread {

    private final Queue<T> items = new LinkedList<>();
    private final Consumer <? super T> finalizeItem;

    @Override
    public void run() {
        while ( !Thread.currentThread().isInterrupted() ) {
            synchronized ( finalizeItem ) {
                while ( !items.isEmpty() ) {
                    try {
                        finalizeItem.accept( items.poll() );
                    } catch ( Exception e ) {
                        System.err.println( "Error finalizing item: " + e.getMessage() );
                    }
                }
                try {
                    finalizeItem.wait();
                } catch ( InterruptedException e ) {
                    Thread.currentThread().interrupt();
                    System.err.println( "Finalizer thread interrupted: " + e.getMessage() );
                }
            }
        }
    }

    public void addItem( T item ) {
        synchronized ( finalizeItem ) {
            items.add( item );
            finalizeItem.notify();
        }
    }

}
