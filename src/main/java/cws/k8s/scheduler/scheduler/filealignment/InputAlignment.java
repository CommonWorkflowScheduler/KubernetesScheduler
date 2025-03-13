package cws.k8s.scheduler.scheduler.filealignment;

import cws.k8s.scheduler.model.NodeWithAlloc;
import cws.k8s.scheduler.model.Task;
import cws.k8s.scheduler.model.taskinputs.TaskInputs;
import cws.k8s.scheduler.util.FileAlignment;
import cws.k8s.scheduler.util.copying.CurrentlyCopyingOnNode;
import org.jetbrains.annotations.NotNull;

public interface InputAlignment {

    /**
     * Calculate a alignment for the input data. Stop if costs are higher than maxCost
     * @param task
     * @param inputsOfTask
     * @param node
     * @param maxCost
     * @return null if no or no better than maxCost alignment is found
     */
    FileAlignment getInputAlignment( @NotNull Task task,
                                     @NotNull TaskInputs inputsOfTask,
                                     @NotNull NodeWithAlloc node,
                                     CurrentlyCopyingOnNode currentlyCopying,
                                     CurrentlyCopyingOnNode currentlyPlanedToCopy,
                                     double maxCost );

    /**
     * Calculate a alignment for the input data
     * @param task
     * @param inputsOfTask
     * @param node
     * @return null if no alignment is found
     */
    default FileAlignment getInputAlignment( @NotNull Task task,
                                             @NotNull TaskInputs inputsOfTask,
                                             @NotNull NodeWithAlloc node,
                                             CurrentlyCopyingOnNode currentlyCopying,
                                             CurrentlyCopyingOnNode currentlyPlanedToCopy
    ){
        return getInputAlignment( task, inputsOfTask, node, currentlyCopying, currentlyPlanedToCopy, Double.MAX_VALUE );
    }

}
