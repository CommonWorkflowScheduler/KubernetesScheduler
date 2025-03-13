package cws.k8s.scheduler.scheduler.schedulingstrategy;

import lombok.RequiredArgsConstructor;
import org.jetbrains.annotations.NotNull;

import java.util.List;

@RequiredArgsConstructor
public class InputEntry implements Comparable<InputEntry> {

    public final String currentIP;
    public final String node;
    public final List<String> files;
    public final long size;

    @Override
    public int compareTo(@NotNull InputEntry o) {
        return Long.compare( size, o.size );
    }
}
