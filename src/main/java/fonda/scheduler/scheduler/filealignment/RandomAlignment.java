package fonda.scheduler.scheduler.filealignment;

import fonda.scheduler.model.NodeWithAlloc;
import fonda.scheduler.model.Task;
import fonda.scheduler.model.location.hierachy.LocationWrapper;
import fonda.scheduler.model.taskinputs.PathFileLocationTriple;
import fonda.scheduler.model.taskinputs.TaskInputs;
import fonda.scheduler.util.FileAlignment;
import fonda.scheduler.util.FilePath;
import org.jetbrains.annotations.NotNull;

import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Random;

public class RandomAlignment implements InputAlignment {

    private final Random random = new Random();

    @Override
    public FileAlignment getInputAlignment(Task task, @NotNull TaskInputs inputsOfTask, NodeWithAlloc node, double maxCost) {
        final HashMap<String, List<FilePath>> map = new HashMap<>();
        for (PathFileLocationTriple pathFileLocationTriple : inputsOfTask.getFiles()) {
            final LocationWrapper locationWrapper = pathFileLocationTriple.locations.get(
                    random.nextInt( pathFileLocationTriple.locations.size() )
            );
            final String nodeIdentifier = locationWrapper.getLocation().getIdentifier();
            final List<FilePath> pathsOfNode = map.computeIfAbsent(nodeIdentifier, k -> new LinkedList<>() );
            pathsOfNode.add( new FilePath( pathFileLocationTriple, locationWrapper ) );
        }
        return new FileAlignment( map, inputsOfTask.getSymlinks(), 0);
    }

}
