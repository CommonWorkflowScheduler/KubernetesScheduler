package fonda.scheduler.util;

import fonda.scheduler.model.location.Location;
import fonda.scheduler.model.location.hierachy.LocationWrapper;
import fonda.scheduler.model.taskinputs.SymlinkInput;
import lombok.Getter;
import lombok.Setter;
import lombok.ToString;

import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

@Getter
@ToString
public class FileAlignment {

    /*
    Key: node
    Value: Files from the node
     */
    private final Map<Location, AlignmentWrapper> nodeFileAlignment;
    private final List<SymlinkInput> symlinks;
    private final double cost;
    @Setter
    private double weight = 1.0;

    public FileAlignment(Map<Location, AlignmentWrapper> nodeFileAlignment, List<SymlinkInput> symlinks, double cost) {
        this.nodeFileAlignment = nodeFileAlignment;
        this.symlinks = symlinks;
        this.cost = cost;
    }

    public double getWorth() {
        return cost / weight;
    }

    /**
     * Check if data is copied from at least one node
     * @param node this node is not checked
     * @return
     */
    public boolean copyFromSomewhere( Location node ) {
        return nodeFileAlignment
                .entrySet()
                .stream()
                .anyMatch( a -> a.getKey() != node && !a.getValue().getFilesToCopy().isEmpty() );
    }

    public List<LocationWrapper> getAllLocationWrappers(){
        return nodeFileAlignment
                .entrySet()
                .parallelStream()
                .flatMap( l -> l
                        .getValue()
                        .getAll()
                        .parallelStream()
                        .map(FilePath::getLocationWrapper)
                )
                .collect(Collectors.toList());
    }
}
