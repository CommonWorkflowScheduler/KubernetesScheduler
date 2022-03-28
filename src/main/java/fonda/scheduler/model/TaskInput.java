package fonda.scheduler.model;

import lombok.ToString;

import java.util.List;

@ToString
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

    /**
     * Only for testing
     * @param booleanInputs
     * @param numberInputs
     * @param stringInputs
     * @param fileInputs
     */
    TaskInput(List<InputParam<Boolean>> booleanInputs, List<InputParam<Number>> numberInputs, List<InputParam<String>> stringInputs, List<InputParam<FileHolder>> fileInputs) {
        this.booleanInputs = booleanInputs;
        this.numberInputs = numberInputs;
        this.stringInputs = stringInputs;
        this.fileInputs = fileInputs;
    }

}
