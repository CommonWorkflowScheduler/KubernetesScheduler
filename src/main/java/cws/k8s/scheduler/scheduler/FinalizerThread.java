package cws.k8s.scheduler.scheduler;

import lombok.RequiredArgsConstructor;

import java.util.LinkedList;
import java.util.Queue;
import java.util.function.Consumer;

public class FinalizerThread<T> extends Thread {

    private final Queue<T> items = new LinkedList<>();
    private final Consumer <? super T> finalizeItem;

    public FinalizerThread(Consumer <? super T> finalizeItem) {
        super( "FinalizerThread" );
        this.finalizeItem = finalizeItem;
    }

    @Override
    public void run() {
        while ( !Thread.currentThread().isInterrupted() ) {
            Queue<T> toProcess = new LinkedList<>();
            synchronized ( finalizeItem ) {
                while ( !items.isEmpty() ) {
                    try {
                        toProcess.add( items.poll() );
                    } catch ( Exception e ) {
                        System.err.println( "Error finalizing item: " + e.getMessage() );
                    }
                }
            }
            for ( T item : toProcess ) {
                try {
                    finalizeItem.accept( item );
                } catch ( Exception e ) {
                    System.err.println( "Error finalizing item: " + e.getMessage() );
                }
            }
            synchronized ( finalizeItem ) {
                if ( items.isEmpty() ) {
                    try {
                        finalizeItem.wait();
                    } catch ( InterruptedException e ) {
                        Thread.currentThread().interrupt();
                        System.err.println( "Finalizer thread interrupted: " + e.getMessage() );
                    }
                }
            }
        }
    }

    public void addItem( T item ) {
        synchronized ( finalizeItem ) {
            items.add( item );
            finalizeItem.notifyAll();
        }
    }

}
