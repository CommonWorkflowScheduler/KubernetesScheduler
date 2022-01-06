package fonda.scheduler.model;

import fonda.scheduler.model.location.hierachy.LocationWrapper;
import fonda.scheduler.model.location.hierachy.RealFile;
import lombok.Getter;

@Getter
public class TaskInputFileLocationWrapper {

    final private RealFile file;
    final private LocationWrapper wrapper;

    public TaskInputFileLocationWrapper(RealFile file, LocationWrapper wrapper) {
        this.file = file;
        this.wrapper = wrapper;
    }

    public void apply(){
        file.addOrUpdateLocation( false, wrapper );
    }

}
