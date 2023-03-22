package cws.k8s.scheduler.scheduler.la2;

import lombok.Getter;
import lombok.RequiredArgsConstructor;

import java.util.Comparator;

@RequiredArgsConstructor
public abstract class TaskStatComparator  implements Comparator<TaskStat> {

    @Getter
    private final Comparator<TaskStat.NodeAndStatWrapper> comparator;


}
