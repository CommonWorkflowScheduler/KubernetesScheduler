package cws.k8s.scheduler.model.tracing;

import lombok.Getter;
import lombok.Setter;

import java.io.BufferedWriter;
import java.io.FileWriter;
import java.io.IOException;
import java.util.List;
import java.util.stream.Collectors;


public class TraceRecord {

    @Getter
    @Setter
    private Integer schedulerDependingTask = null;

    @Getter
    @Setter
    private Long schedulerTimeInQueue = null;

    @Getter
    @Setter
    private Integer schedulerPlaceInQueue = null;

    @Getter
    @Setter
    private Integer schedulerNodesTried = null;

    @Getter
    @Setter
    private List<Double> schedulerNodesCost = null;

    @Getter
    @Setter
    private Integer schedulerCouldStopFetching = null;

    @Getter
    @Setter
    private Double schedulerBestCost = null;

    private int schedulerTriedToSchedule = 0;

    @Getter
    @Setter
    private Integer schedulerTimeToSchedule = null;

    private Integer schedulerDeltaScheduleSubmitted = null;

    private Integer schedulerDeltaScheduleAlignment = null;

    @Getter
    @Setter
    /*ID of the batch*/
    private Integer schedulerBatchId = null;

    @Getter
    @Setter
    /*Time delta between a batch was started and the scheduler received this task from the workflow engine*/
    private Integer schedulerDeltaBatchStartSubmitted = null;

    @Getter
    @Setter
    /*Time delta between a batch was started and the scheduler received the pod from the k8s API*/
    private Integer schedulerDeltaBatchStartReceived = null;

    @Getter
    @Setter
    /*Time delta between a batch was closed by the workflow engine and the scheduler received the pod from the k8s API*/
    private Integer schedulerDeltaBatchClosedBatchEnd = null;

    @Getter
    @Setter
    /*Time delta between a task was submitted and the batch became scheduable*/
    private Integer schedulerDeltaSubmittedBatchEnd = null;

    public void writeRecord( String tracePath ) throws IOException {

        try ( BufferedWriter bw = new BufferedWriter( new FileWriter( tracePath ) ) ) {
            bw.write("nextflow.scheduler.trace/v1\n");
            writeValue("scheduler_depending_task", schedulerDependingTask, bw);
            writeValue("scheduler_time_in_queue", schedulerTimeInQueue, bw);
            writeValue("scheduler_place_in_queue", schedulerPlaceInQueue, bw);
            writeValue("scheduler_nodes_tried", schedulerNodesTried, bw);
            writeValue("scheduler_nodes_cost", schedulerNodesCost, bw);
            writeValue("scheduler_could_stop_fetching", schedulerCouldStopFetching, bw);
            writeValue("scheduler_best_cost", schedulerBestCost, bw);
            writeValue("scheduler_tried_to_schedule", schedulerTriedToSchedule, bw);
            writeValue("scheduler_time_to_schedule", schedulerTimeToSchedule, bw);
            writeValue("scheduler_delta_schedule_submitted", schedulerDeltaScheduleSubmitted, bw);
            writeValue("scheduler_delta_schedule_alignment", schedulerDeltaScheduleAlignment, bw);
            writeValue("scheduler_batch_id", schedulerBatchId, bw);
            writeValue("scheduler_delta_batch_start_submitted", schedulerDeltaBatchStartSubmitted, bw);
            writeValue("scheduler_delta_batch_start_received", schedulerDeltaBatchStartReceived, bw);
            writeValue("scheduler_delta_batch_closed_batch_end", schedulerDeltaBatchClosedBatchEnd, bw);
            writeValue("scheduler_delta_submitted_batch_end", schedulerDeltaSubmittedBatchEnd, bw);

        }

    }

    private <T extends Number> void writeValue( String name, T value, BufferedWriter bw ) throws IOException {
        if ( value != null ) {
            bw.write( name + '=' + value + '\n' );
        }
    }

    private void writeValue( String name, List<Double> value, BufferedWriter bw ) throws IOException {
        if ( value != null ) {
            final String collect = value.stream()
                    .map( x -> x==null ? "null" : x.toString() )
                    .collect(Collectors.joining(";"));
            bw.write( name + "=\"" + collect + "\"\n" );
        }
    }

    private long startSchedule = 0;

    /**
     * The task was submitted to a node
     */
    public void submitted() {
        schedulerDeltaScheduleSubmitted = (int) (System.currentTimeMillis() - startSchedule);
    }

    /**
     * Created an alignment for the task
     */
    public void foundAlignment() {
        schedulerDeltaScheduleAlignment = (int) (System.currentTimeMillis() - startSchedule);
    }

    public void tryToSchedule( long startSchedule ){
        this.startSchedule = startSchedule;
        schedulerTriedToSchedule++;
    }


}
