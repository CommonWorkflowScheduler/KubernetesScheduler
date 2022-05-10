package fonda.scheduler.scheduler.schedulingstrategy;

import org.jetbrains.annotations.NotNull;

import java.util.List;

public class InputEntry implements Comparable<InputEntry> {

    public final String currentIP;
    public final String node;
    public final List<String> files;
    public final long size;

    public InputEntry( String currentIP, String node, List<String> files, long size ) {
        this.currentIP = currentIP;
        this.node = node;
        this.files = files;
        this.size = size;
    }

    @Override
    public int compareTo(@NotNull InputEntry o) {
        return Long.compare( size, o.size );
    }
}
