package cws.k8s.scheduler.scheduler.outlabel;

import cws.k8s.scheduler.model.Task;

import java.util.Set;

public class HolderMaxTasks extends OutLabelHolder {

    @Override
    protected InternalHolderMaxTasks create() {
        return new InternalHolderMaxTasks();
    }

    private class InternalHolderMaxTasks extends InternalHolder {

        @Override
        protected double calculateValue( Set<Task> input ) {
            return input.size();
        }

    }
}
