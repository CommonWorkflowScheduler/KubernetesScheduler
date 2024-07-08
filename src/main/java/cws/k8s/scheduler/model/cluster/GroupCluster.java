package cws.k8s.scheduler.model.cluster;

import cws.k8s.scheduler.model.Task;
import cws.k8s.scheduler.model.location.NodeLocation;

import java.util.*;

public abstract class GroupCluster {

    protected final Set<String> allLabels = new HashSet<>();
    protected final Map<String,LabelCount> countPerLabel = new HashMap<>();
    protected final LinkedList<Task> unscheduledTasks = new LinkedList<>();
    protected final LinkedList<Task> scheduledTasks = new LinkedList<>();
    protected final LinkedList<Task> finishedTasks = new LinkedList<>();
    protected final Map<String,NodeLocation> labelToNode = new HashMap<>();
    protected final Map<NodeLocation,Set<String>> nodeToLabel = new HashMap<>();

    private void addAllOutLabels( List<Task> tasks ) {
        for ( Task task : tasks ) {
            if ( task.getOutLabel() != null ) {
                allLabels.addAll( task.getOutLabel() );
            }
        }
    }

    public void tasksBecameAvailable( List<Task> tasks ) {
        boolean anyWithLabel = false;
        synchronized ( this ) {
            unscheduledTasks.addAll( tasks );
            addAllOutLabels( tasks );
            for ( Task task : tasks ) {
                if ( task.getOutLabel() == null || task.getOutLabel().isEmpty() ) {
                    continue;
                }
                for ( String label : task.getOutLabel() ) {
                    anyWithLabel = true;
                    countPerLabel.computeIfAbsent( label, k -> new LabelCount() ).addWaitingTask( task );
                    final LabelCount labelCount;
                    if ( !countPerLabel.containsKey( label ) ) {
                        labelCount = new LabelCount();
                        countPerLabel.put( label, labelCount );
                    } else {
                        labelCount = countPerLabel.get( label );
                    }
                    labelCount.addWaitingTask( task );
                }
            }
            if ( anyWithLabel ) {
                recalculate();
            }
        }
    }

    public void taskWasAssignedToNode( Task task ) {
        synchronized ( this ) {
            unscheduledTasks.remove( task );
            scheduledTasks.add( task );
            if ( task.getOutLabel() != null ) {
                for ( String label : task.getOutLabel() ) {
                    countPerLabel.get( label ).makeTaskRunning( task );
                }
                recalculate();
            }
        }
    }

    public void tasksHaveFinished( List<Task> tasks ) {
        synchronized ( this ) {
            scheduledTasks.removeAll( tasks );
            finishedTasks.addAll( tasks );
            for ( Task task : tasks ) {
                if ( task.getOutLabel() == null ) {
                    continue;
                }
                for ( String label : task.getOutLabel() ) {
                    if ( countPerLabel.get( label ) == null ) {
                        continue;
                    }
                    countPerLabel.get( label ).makeTaskFinished( task );
                }
            }
        }
    }

    abstract void recalculate();

    public double getScoreForTaskOnNode( Task task, NodeLocation nodeLocation ) {
        if ( task.getOutLabel() == null || task.getOutLabel().isEmpty() ) {
            return 1;
        }
        //Jaccard similarity coefficient
        final Set<String> outLabel = new HashSet<>( task.getOutLabel() );
        int outLabelSize = outLabel.size();
        final Set<String> nodeLabels = nodeToLabel.get( nodeLocation );
        if ( nodeLabels == null ) {
            return 0.01;
        }
        outLabel.retainAll( nodeLabels );
        if ( outLabelSize == 0 ) {
            return 1;
        }
        return (double) outLabel.size() / outLabelSize + 0.01; // to avoid zero
    }

    protected void addNodeToLabel( NodeLocation nodeLocation, String label ) {
        final NodeLocation currentLocation = labelToNode.get( label );

        if ( nodeLocation.equals( currentLocation ) ) {
            return;
        }

        Set<String> labels = nodeToLabel.computeIfAbsent( nodeLocation, k -> new HashSet<>() );
        labels.add( label );

        if ( currentLocation != null ) {
            labels = nodeToLabel.get( currentLocation );
            labels.remove( label );
        }
        labelToNode.put( label, nodeLocation );
    }

}
