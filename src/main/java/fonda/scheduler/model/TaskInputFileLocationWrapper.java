package fonda.scheduler.model;

import fonda.scheduler.model.location.hierachy.LocationWrapper;
import fonda.scheduler.model.location.hierachy.RealHierarchyFile;
import lombok.Getter;

@Getter
public class TaskInputFileLocationWrapper {

    private final RealHierarchyFile file;
    private final LocationWrapper wrapper;

    public TaskInputFileLocationWrapper(RealHierarchyFile file, LocationWrapper wrapper) {
        this.file = file;
        this.wrapper = wrapper;
    }

    public void apply(){
        file.addOrUpdateLocation( false, wrapper );
    }

}
