package cws.k8s.scheduler.scheduler.la2.copyinadvance;

import cws.k8s.scheduler.model.Requirements;
import lombok.AllArgsConstructor;
import lombok.RequiredArgsConstructor;

import java.util.List;
import java.util.Optional;

@RequiredArgsConstructor
public class ShouldCopyChecker {

    private final long taskScore;
    private final List<CopyInAdvanceNodeWithMostDataIntelligent.TaskWithScore> waiting;
    private final Requirements taskRequest;

    boolean couldBeStarted( Requirements becomesAvailable ) {
        //True if the task to check would be started in the current case
        Result results = new Result( true, taskScore );
        final Optional<Result> reduce = waiting.parallelStream().map( waitingTask -> {
            final Result result = new Result( true, taskScore );
            final Requirements waitingRequest = waitingTask.task.getPlanedRequirements();
            //In all cases waitingRequest is used and accordingly the request must always be smaller than the available resources
            if ( waitingRequest.smallerEquals( becomesAvailable ) ) {
                checkSingle( becomesAvailable, result, waitingRequest.add( taskRequest ), waitingTask.score );
            }
            return result;
        } ).reduce( ( a, b ) -> a.bestScore > b.bestScore ? a : b );
        return reduce.map( result -> result.couldBeStarted ).orElseGet( () -> results.couldBeStarted );
    }

    /**
     *
     * @param becomesAvailable resources available
     * @param results wrapper for the result
     * @param requestAndTaskRequest sum of task to check and waiting task
     * @param score score of the waiting task
     */
    private void checkSingle( Requirements becomesAvailable, Result results, Requirements requestAndTaskRequest, long score ) {
        if ( results.bestScore < score ) {
            results.bestScore = score;
            results.couldBeStarted = false;
        }
        if ( results.bestScore < score + taskScore && requestAndTaskRequest.smallerEquals( becomesAvailable ) ) {
            results.bestScore = score + taskScore;
            results.couldBeStarted = true;
        }
    }

    @AllArgsConstructor
    private static class Result {
        boolean couldBeStarted;
        long bestScore;
    }

}
