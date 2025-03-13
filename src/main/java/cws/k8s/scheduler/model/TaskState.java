package cws.k8s.scheduler.model;

import lombok.Getter;
import lombok.Setter;

public class TaskState {

    @Getter
    @Setter
    private State state = State.RECEIVED_CONFIG;

    @Getter
    private String error;

    public void error (String error){
        this.state = State.ERROR;
        this.error = error;
    }

}

