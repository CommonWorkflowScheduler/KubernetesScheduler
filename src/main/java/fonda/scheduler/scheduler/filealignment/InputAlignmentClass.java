package fonda.scheduler.scheduler.filealignment;

import fonda.scheduler.model.NodeWithAlloc;
import fonda.scheduler.model.Task;
import fonda.scheduler.model.location.Location;
import fonda.scheduler.model.location.NodeLocation;
import fonda.scheduler.model.location.hierachy.LocationWrapper;
import fonda.scheduler.model.taskinputs.PathFileLocationTriple;
import fonda.scheduler.model.taskinputs.TaskInputs;
import fonda.scheduler.scheduler.filealignment.costfunctions.NoAligmentPossibleException;
import fonda.scheduler.util.AlignmentWrapper;
import fonda.scheduler.util.FileAlignment;
import fonda.scheduler.util.FilePathWithTask;
import fonda.scheduler.util.Tuple;
import fonda.scheduler.util.copying.CopySource;
import fonda.scheduler.util.copying.CurrentlyCopyingOnNode;
import lombok.extern.slf4j.Slf4j;
import org.jetbrains.annotations.NotNull;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

@Slf4j
abstract class InputAlignmentClass implements InputAlignment {

    /**
     * Check if another scheduled task is already copying a required file
     *
     * @param currentlyCopying
     * @param path
     * @param locations
     * @return
     */
    private Tuple<LocationWrapper,Task> alreadyCopying( CurrentlyCopyingOnNode currentlyCopying, String path, List<LocationWrapper> locations, String debug ){
        if ( currentlyCopying != null && currentlyCopying.isCurrentlyCopying( path )  ){
            final CopySource copySource = currentlyCopying.getCopySource( path );
            final Location copyFrom = copySource.getLocation();
            for ( LocationWrapper locationWrapper : locations ) {
                if ( locationWrapper.getLocation() == copyFrom ) {
                    log.info( "Checking if {} is already copying: {} (Found)", path, currentlyCopying.getAllFilesCurrentlyCopying() );
                    return new Tuple(locationWrapper,copySource.getTask());
                }
            }
            throw new NoAligmentPossibleException( "Node is a already copying file: " + path + " but in an incompatible version." );
        }
        log.info( "Checking if {} is already copying: {} (Not found)", path, currentlyCopying == null ? null : currentlyCopying.getAllFilesCurrentlyCopying() );
        return null;
    }

    private boolean canUseFileFromOtherTask (
            CurrentlyCopyingOnNode currentlyCopying,
            PathFileLocationTriple pathFileLocationTriple,
            Map<Location, AlignmentWrapper> map,
            String debug
    ) {
        final Tuple<LocationWrapper, Task> copying = alreadyCopying(
                currentlyCopying,
                pathFileLocationTriple.path.toString(),
                pathFileLocationTriple.locations,
                debug
        );
        if ( copying != null ) {
            final AlignmentWrapper alignmentWrapper = map.computeIfAbsent(copying.getA().getLocation(), k -> new AlignmentWrapper());
            alignmentWrapper.addAlignmentToWaitFor(new FilePathWithTask(pathFileLocationTriple, copying.getA(), copying.getB()), copying.getA().getSizeInBytes());
            return true;
        }
        return false;
    }

    @Override
    public FileAlignment getInputAlignment(@NotNull Task task,
                                           @NotNull TaskInputs inputsOfTask,
                                           @NotNull NodeWithAlloc node,
                                           CurrentlyCopyingOnNode currentlyCopying,
                                           CurrentlyCopyingOnNode currentlyPlanedToCopy,
                                           double maxCost) {
        final HashMap<Location, AlignmentWrapper> map = new HashMap<>();
        double cost = 0;
        for (PathFileLocationTriple pathFileLocationTriple : inputsOfTask.getFiles()) {
            if ( !canUseFileFromOtherTask( currentlyCopying, pathFileLocationTriple, map, "copying" )
                    &&
                    !canUseFileFromOtherTask( currentlyPlanedToCopy, pathFileLocationTriple, map, "currentSchedule" )
            ) {
                final double newCost = findAlignmentForFile( pathFileLocationTriple, node.getNodeLocation(), map );
                if ( newCost > maxCost ) {
                    return null;
                }
                if ( newCost > cost ) {
                    cost = newCost;
                }
            }
        }
        return new FileAlignment( map, inputsOfTask.getSymlinks(), cost);
    }

    abstract double findAlignmentForFile(PathFileLocationTriple pathFileLocationTriple, NodeLocation scheduledNode, HashMap<Location, AlignmentWrapper> map);

}
