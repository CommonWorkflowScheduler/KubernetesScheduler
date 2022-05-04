package fonda.scheduler.scheduler.filealignment;

import fonda.scheduler.model.NodeWithAlloc;
import fonda.scheduler.model.Task;
import fonda.scheduler.model.location.hierachy.LocationWrapper;
import fonda.scheduler.model.taskinputs.PathFileLocationTriple;
import fonda.scheduler.model.taskinputs.TaskInputs;
import fonda.scheduler.util.AlignmentWrapper;
import fonda.scheduler.util.FileAlignment;
import fonda.scheduler.util.FilePath;
import org.jetbrains.annotations.NotNull;

import java.util.HashMap;
import java.util.Random;

public class RandomAlignment implements InputAlignment {

    private final Random random = new Random();

    @Override
    public FileAlignment getInputAlignment(Task task, @NotNull TaskInputs inputsOfTask, NodeWithAlloc node, double maxCost) {
        final HashMap<String, AlignmentWrapper> map = new HashMap<>();
        for (PathFileLocationTriple pathFileLocationTriple : inputsOfTask.getFiles()) {
            final LocationWrapper locationWrapper = pathFileLocationTriple.locations.get(
                    random.nextInt( pathFileLocationTriple.locations.size() )
            );
            final String nodeIdentifier = locationWrapper.getLocation().getIdentifier();
            final AlignmentWrapper alignmentWrapper = map.computeIfAbsent(nodeIdentifier, k -> new AlignmentWrapper() );
            alignmentWrapper.addAlignment( new FilePath( pathFileLocationTriple, locationWrapper ), 0 );
        }
        return new FileAlignment( map, inputsOfTask.getSymlinks(), 0);
    }

}
