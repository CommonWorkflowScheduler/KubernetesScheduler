package fonda.scheduler.util;

import lombok.Getter;

import java.util.LinkedList;
import java.util.List;

@Getter
public class AlignmentWrapper {

    private final List<FilePath> alignment = new LinkedList<>();
    private double cost = 0;

    public void addAlignment( FilePath filePath, double cost ) {
        alignment.add( filePath );
        this.cost = cost;
    }

    public boolean empty() {
        return alignment.isEmpty();
    }

}
