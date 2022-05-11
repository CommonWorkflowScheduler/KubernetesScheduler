package fonda.scheduler.util;

import lombok.Getter;

import java.util.LinkedList;
import java.util.List;

public class AlignmentWrapper {

    @Getter
    private final List<FilePath> filesToCopy = new LinkedList<>();

    private final List<FilePathWithTask> waitFor = new LinkedList<>();

    @Getter
    private double cost = 0;

    public void addAlignmentToCopy( FilePath filePath, double cost ) {
        filesToCopy.add( filePath );
        this.cost = cost;
    }

    public void addAlignmentToWaitFor( FilePathWithTask filePath, double cost ) {
        waitFor.add( filePath );
        this.cost = cost;
    }

    public void addAlignmentToWaitFor( FilePathWithTask filePath ) {
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
