package fonda.scheduler.model;

import fonda.scheduler.model.location.hierachy.LocationWrapper;
import fonda.scheduler.model.location.hierachy.RealHierarchyFile;
import lombok.Getter;
import lombok.RequiredArgsConstructor;

@Getter
@RequiredArgsConstructor
public class TaskInputFileLocationWrapper {

    private final String path;
    private final RealHierarchyFile file;
    private final LocationWrapper wrapper;

    public void success(){
        file.addOrUpdateLocation( false, wrapper );
    }

    public void failure(){
        file.removeLocation( wrapper );
    }

}
