package fonda.scheduler.scheduler.filealignment;

import fonda.scheduler.model.NodeWithAlloc;
import fonda.scheduler.model.Task;
import fonda.scheduler.model.taskinputs.TaskInputs;
import fonda.scheduler.util.FileAlignment;
import org.jetbrains.annotations.NotNull;

public interface InputAlignment {

    FileAlignment getInputAlignment(Task task, @NotNull TaskInputs inputsOfTask, NodeWithAlloc node);

}
