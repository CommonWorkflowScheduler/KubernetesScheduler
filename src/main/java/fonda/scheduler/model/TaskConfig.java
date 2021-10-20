package fonda.scheduler.model;

import lombok.Getter;
import lombok.ToString;

import java.util.List;
import java.util.Map;

@Getter
@ToString
public class TaskConfig {

    private final String task;
    private final String name;
    private final Map< String, List<Object>> schedulerParams;
    private final List<Input> inputs;
    private final String hash;

    private TaskConfig() {
        this.name = null;
        this.schedulerParams = null;
        this.inputs = null;
        this.hash = null;
        this.task = null;
    }

    @Getter
    @ToString
    static public class Input {

        private final String name;
        private final Object value;

        private Input() {
            this.value = null;
            name = null;
        }
    }
}
