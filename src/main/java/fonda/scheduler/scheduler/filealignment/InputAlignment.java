package fonda.scheduler.scheduler.filealignment;

import fonda.scheduler.model.NodeWithAlloc;
import fonda.scheduler.model.Task;
import fonda.scheduler.model.taskinputs.TaskInputs;
import fonda.scheduler.util.FileAlignment;
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
    FileAlignment getInputAlignment( @NotNull Task task, @NotNull TaskInputs inputsOfTask, @NotNull NodeWithAlloc node, double maxCost );

    /**
     * Calculate a alignment for the input data
     * @param task
     * @param inputsOfTask
     * @param node
     * @return null if no alignment is found
     */
    default FileAlignment getInputAlignment( @NotNull Task task, @NotNull TaskInputs inputsOfTask, @NotNull NodeWithAlloc node ){
        return getInputAlignment( task, inputsOfTask, node, Double.MAX_VALUE );
    }

}
