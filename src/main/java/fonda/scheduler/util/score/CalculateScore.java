package fonda.scheduler.util.score;

import fonda.scheduler.scheduler.data.TaskInputsNodes;

public interface CalculateScore {

    /**
     * Score must be higher than 0
     * @return
     */
    long getScore( TaskInputsNodes taskInputsNodes );

}
