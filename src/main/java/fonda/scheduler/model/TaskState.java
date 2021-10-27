package fonda.scheduler.model;

import lombok.Getter;
import lombok.Setter;

public class TaskState {

    @Getter
    @Setter
    private State state;

    @Getter
    private String error;

    public TaskState() {
        this.state = State.RECEIVED_CONFIG;
    }

    public void error (String error){
        this.state = State.ERROR;
        this.error = error;
    }
}

