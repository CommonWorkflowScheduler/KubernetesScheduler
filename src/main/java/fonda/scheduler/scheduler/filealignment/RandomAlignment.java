package fonda.scheduler.scheduler.filealignment;

import fonda.scheduler.model.NodeWithAlloc;
import fonda.scheduler.model.Task;
import fonda.scheduler.model.location.Location;
import fonda.scheduler.model.location.hierachy.LocationWrapper;
import fonda.scheduler.model.taskinputs.PathFileLocationTriple;
import fonda.scheduler.model.taskinputs.TaskInputs;
import fonda.scheduler.util.AlignmentWrapper;
import fonda.scheduler.util.FileAlignment;
import fonda.scheduler.util.FilePath;
import fonda.scheduler.util.Tuple;
import org.jetbrains.annotations.NotNull;

import java.util.HashMap;
import java.util.Map;
import java.util.Optional;
import java.util.Random;

public class RandomAlignment implements InputAlignment {

    private final Random random = new Random();

    @Override
    public FileAlignment getInputAlignment(@NotNull Task task,
                                           @NotNull TaskInputs inputsOfTask,
                                           @NotNull NodeWithAlloc node,
                                           Map<String, Tuple<Task, Location>> currentlyCopying,
                                           double maxCost) {
        final HashMap<Location, AlignmentWrapper> map = new HashMap<>();
        for (PathFileLocationTriple pathFileLocationTriple : inputsOfTask.getFiles()) {
            final Optional<LocationWrapper> first = pathFileLocationTriple
                    .locations
                    .stream()
                    .filter(x -> x.getLocation() == node.getNodeLocation())
                    .findFirst();
            final LocationWrapper locationWrapper = first.orElseGet(() -> pathFileLocationTriple.locations.get(
                    random.nextInt(pathFileLocationTriple.locations.size())
            ));
            final Location location = locationWrapper.getLocation();
            final AlignmentWrapper alignmentWrapper = map.computeIfAbsent(location, k -> new AlignmentWrapper() );
            alignmentWrapper.addAlignment( new FilePath( pathFileLocationTriple, locationWrapper ), 0 );
        }
        return new FileAlignment( map, inputsOfTask.getSymlinks(), 0);
    }

}
