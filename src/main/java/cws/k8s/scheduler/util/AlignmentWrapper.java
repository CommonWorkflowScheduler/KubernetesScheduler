package cws.k8s.scheduler.util;

import lombok.Getter;
import lombok.ToString;

import java.util.LinkedList;
import java.util.List;

@Getter
@ToString
public class AlignmentWrapper {

    private final List<FilePath> filesToCopy = new LinkedList<>();
    private long toCopySize = 0;
    private final List<FilePathWithTask> waitFor = new LinkedList<>();
    private long toWaitSize = 0;
    private double cost = 0;

    public void addAlignmentToCopy( FilePath filePath, double cost, long size ) {
        filesToCopy.add( filePath );
        toCopySize += size;
        this.cost = cost;
    }

    public void addAlignmentToWaitFor( FilePathWithTask filePath, double cost, long size ) {
        addAlignmentToWaitFor( filePath, size );
        this.cost = cost;
    }

    public void addAlignmentToWaitFor( FilePathWithTask filePath, long size ) {
        toWaitSize += size;
        waitFor.add( filePath );
    }

    public boolean empty() {
        return filesToCopy.isEmpty() && waitFor.isEmpty();
    }

    public List<FilePath> getAll() {
        List<FilePath> newList = new LinkedList<>(filesToCopy);
        newList.addAll(waitFor);
        return newList;
    }

}
