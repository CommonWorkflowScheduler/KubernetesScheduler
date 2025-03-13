package cws.k8s.scheduler.model;

import lombok.AccessLevel;
import lombok.NoArgsConstructor;
import lombok.RequiredArgsConstructor;

import java.util.List;

@NoArgsConstructor( access = AccessLevel.PRIVATE, force = true )
/**
 * Only for testing
 */
@RequiredArgsConstructor( access = AccessLevel.PACKAGE )
public class TaskInput {

    public final List<InputParam<Boolean>> booleanInputs;
    public final List<InputParam<Number>> numberInputs;
    public final List<InputParam<String>> stringInputs;
    public final List<InputParam<FileHolder>> fileInputs;

    @Override
    public String toString() {
        return "TaskInput{" +
                "booleanInputs=" + booleanInputs +
                ", numberInputs=" + numberInputs +
                ", stringInputs=" + stringInputs +
                ", fileInputs=#" + (fileInputs != null ?  fileInputs.size() : 0) +
                '}';
    }

}
