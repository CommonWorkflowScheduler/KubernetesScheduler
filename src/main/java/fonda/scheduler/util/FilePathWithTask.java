package fonda.scheduler.util;

import fonda.scheduler.model.Task;
import fonda.scheduler.model.location.hierachy.LocationWrapper;
import fonda.scheduler.model.taskinputs.PathFileLocationTriple;
import lombok.Getter;

public class FilePathWithTask extends FilePath {

    @Getter
    private final Task task;

    public FilePathWithTask(PathFileLocationTriple pathFileLocationTriple, LocationWrapper locationWrapper, Task task ) {
        super(pathFileLocationTriple, locationWrapper);
        this.task = task;
    }

}
