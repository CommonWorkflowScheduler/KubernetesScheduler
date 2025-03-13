package cws.k8s.scheduler.scheduler.filealignment;

import cws.k8s.scheduler.model.location.Location;
import cws.k8s.scheduler.model.location.NodeLocation;
import cws.k8s.scheduler.model.location.hierachy.LocationWrapper;
import cws.k8s.scheduler.model.taskinputs.PathFileLocationTriple;
import cws.k8s.scheduler.util.AlignmentWrapper;
import cws.k8s.scheduler.util.FilePath;

import java.util.HashMap;
import java.util.Optional;
import java.util.Random;

public class RandomAlignment extends InputAlignmentClass {

    private final Random random = new Random();

    public RandomAlignment() {
        super( 0 );
    }

    @Override
    Costs findAlignmentForFile (
            PathFileLocationTriple pathFileLocationTriple,
            NodeLocation scheduledNode,
            HashMap<Location, AlignmentWrapper> map,
            final Costs costs
    ){
        final Optional<LocationWrapper> first = pathFileLocationTriple
                .locations
                .stream()
                .filter(x -> x.getLocation() == scheduledNode)
                .findFirst();
        final LocationWrapper locationWrapper = first.orElseGet(() -> pathFileLocationTriple.locations.get(
                random.nextInt(pathFileLocationTriple.locations.size())
        ));
        final Location location = locationWrapper.getLocation();
        final AlignmentWrapper alignmentWrapper = map.computeIfAbsent(location, k -> new AlignmentWrapper() );
        alignmentWrapper.addAlignmentToCopy( new FilePath( pathFileLocationTriple, locationWrapper ), 0, locationWrapper.getSizeInBytes() );
        final double calculatedCost = costs.getSumOfCosts() + locationWrapper.getSizeInBytes();
        return new Costs( 0, calculatedCost, calculatedCost );
    }

}
