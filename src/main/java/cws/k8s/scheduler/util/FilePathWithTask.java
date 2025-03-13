package cws.k8s.scheduler.util;

import cws.k8s.scheduler.model.Task;
import cws.k8s.scheduler.model.location.hierachy.LocationWrapper;
import cws.k8s.scheduler.model.taskinputs.PathFileLocationTriple;
import lombok.Getter;

public class FilePathWithTask extends FilePath {

    @Getter
    private final Task task;

    public FilePathWithTask(PathFileLocationTriple pathFileLocationTriple, LocationWrapper locationWrapper, Task task ) {
        super(pathFileLocationTriple, locationWrapper);
        this.task = task;
    }

}
