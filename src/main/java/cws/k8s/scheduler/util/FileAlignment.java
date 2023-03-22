package cws.k8s.scheduler.util;

import cws.k8s.scheduler.model.location.Location;
import cws.k8s.scheduler.model.location.hierachy.LocationWrapper;
import cws.k8s.scheduler.model.taskinputs.SymlinkInput;
import lombok.Getter;
import lombok.RequiredArgsConstructor;
import lombok.Setter;
import lombok.ToString;

import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

@Getter
@ToString
@RequiredArgsConstructor
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
