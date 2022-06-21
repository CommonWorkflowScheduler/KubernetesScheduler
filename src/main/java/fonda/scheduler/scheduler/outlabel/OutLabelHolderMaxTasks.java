package fonda.scheduler.scheduler.outlabel;

import fonda.scheduler.model.Task;
import fonda.scheduler.model.location.NodeLocation;

import java.util.Map;
import java.util.Set;

public class OutLabelHolderMaxTasks extends OutLabelHolder {

    @Override
    protected NodeLocation determineBestNode(Set<Map.Entry<NodeLocation, Set<Task>>> set) {
        int maxNumberOfTask = -1;
        NodeLocation bestLocation = null;
        for (Map.Entry<NodeLocation, Set<Task>> nodeLocationSetEntry : set) {
            final int size = nodeLocationSetEntry.getValue().size();
            if ( size > maxNumberOfTask ) {
                maxNumberOfTask = size;
                bestLocation = nodeLocationSetEntry.getKey();
            }
        }
        return bestLocation;
    }

}
