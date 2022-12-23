package fonda.scheduler.util;

import java.io.BufferedWriter;
import java.io.FileWriter;
import java.io.IOException;

public class LogCopyTask {

    private final BufferedWriter writer;

    public LogCopyTask() {
        try {
            this.writer = new BufferedWriter( new FileWriter( "/pvcdata/scheduler/copytasks.csv" ) );
        } catch ( IOException e ) {
            throw new RuntimeException( e );
        }
    }

    public void log( String text ) {
        try {
            writer.write( text );
            writer.newLine();
            writer.flush();
        } catch ( IOException e ) {
            throw new RuntimeException( e );
        }
    }

    public void copy( String task, String node, int filesToCopy, String additional ) {
        String str = "\"" + task + "\";\"" + node + "\";\"" + filesToCopy+ "\";\"" + additional + "\";" + System.currentTimeMillis();
        synchronized ( writer ) {
            try {
                writer.write( str );
                writer.newLine();
                writer.flush();
            } catch ( IOException e ) {
                throw new RuntimeException( e );
            }
        }
    }

    public void close() {
        try {
            writer.close();
        } catch ( IOException e ) {
            throw new RuntimeException( e );
        }
    }

}
