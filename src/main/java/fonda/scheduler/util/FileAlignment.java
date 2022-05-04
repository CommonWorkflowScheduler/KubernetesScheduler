package fonda.scheduler.util;

import fonda.scheduler.model.location.hierachy.LocationWrapper;
import fonda.scheduler.model.taskinputs.SymlinkInput;

import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

public class FileAlignment {

    /*
    Key: node
    Value: Files from the node
     */
    public final Map<String, List<FilePath>> nodeFileAlignment;
    public final List<SymlinkInput> symlinks;
    public final double cost;

    public FileAlignment(Map<String, List<FilePath>> nodeFileAlignment, List<SymlinkInput> symlinks, double cost) {
        this.nodeFileAlignment = nodeFileAlignment;
        this.symlinks = symlinks;
        this.cost = cost;
    }

    public List<LocationWrapper> getAllLocationWrappers(){
        return nodeFileAlignment
                .entrySet()
                .parallelStream()
                .flatMap( l -> l
                        .getValue()
                        .parallelStream()
                        .map( p -> p.getLocationWrapper() )
                )
                .collect(Collectors.toList());
    }
}
