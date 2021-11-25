package fonda.scheduler.model;

import java.util.List;

public class TaskInput {

    public final List<InputParam<Boolean>> booleanInputs;
    public final List<InputParam<Number>> numberInputs;
    public final List<InputParam<String>> stringInputs;
    public final List<InputParam<FileHolder>> fileInputs;

    private TaskInput() {
        this.booleanInputs = null;
        this.numberInputs  = null;
        this.stringInputs  = null;
        this.fileInputs    = null;
    }

    @Override
    public String toString() {
        return "TaskInput{" +
                "booleanInputs=" + booleanInputs +
                ", numberInputs=" + numberInputs +
                ", stringInputs=" + stringInputs +
                ", fileInputs=" + fileInputs +
                '}';
    }
}
