package fonda.scheduler.scheduler.data;

import fonda.scheduler.model.Task;
import fonda.scheduler.scheduler.MatchingFilesAndNodes;
import lombok.Getter;
import org.jetbrains.annotations.NotNull;

import java.util.List;

@Getter
public class TaskData implements Comparable<TaskData> {
        private final double value;
        private final Task task;
        private final List<NodeDataTuple> nodeDataTuples;
        private final MatchingFilesAndNodes matchingFilesAndNodes;
        private long timeInNs = 0;

        public TaskData(
                double value,
                Task task,
                List<NodeDataTuple> nodeDataTuples,
                MatchingFilesAndNodes matchingFilesAndNodes
        ) {
            this.value = value;
            this.task = task;
            this.nodeDataTuples = nodeDataTuples;
            this.matchingFilesAndNodes = matchingFilesAndNodes;
        }

        @Override
        public int compareTo(@NotNull TaskData o) {
            return Double.compare(value, o.value);
        }

        public void addNs( long timeInNs ){
            this.timeInNs += timeInNs;
        }
    }