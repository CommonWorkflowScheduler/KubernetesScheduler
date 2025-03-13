package cws.k8s.scheduler.model.tracing;

import lombok.Getter;
import lombok.Setter;

import java.io.BufferedWriter;
import java.io.FileWriter;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;


public class TraceRecord {

    @Getter
    @Setter
    /*Filesize required for task*/
    private Long schedulerFilesBytes = null;

    @Getter
    @Setter
    /*Filesize required for task and already on node*/
    private Long schedulerFilesNodeBytes = null;

    @Getter
    @Setter
    /*Filesize required for task and already copied by other task*/
    private Long schedulerFilesNodeOtherTaskBytes = null;

    @Getter
    @Setter
    private Integer schedulerFiles = null;

    @Getter
    @Setter
    private Integer schedulerFilesNode = null;

    @Getter
    @Setter
    private Integer schedulerFilesNodeOtherTask = null;

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
    private Integer schedulerLocationCount = null;

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
    private Integer schedulerNodesToCopyFrom = null;

    @Getter
    @Setter
    private Integer schedulerTimeToSchedule = null;

    @Getter
    @Setter
    private Integer schedulerNoAlignmentFound = null;

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
    /*Time delta between a task was submitted and the batch became schedulable*/
    private Integer schedulerDeltaSubmittedBatchEnd = null;

    @Getter
    private List<Integer> schedulerTimeDeltaPhaseThree = null;

    private int schedulerCopyTasks = 0;

    public void addSchedulerTimeDeltaPhaseThree( Integer schedulerTimeDeltaPhaseThree ) {
        if ( this.schedulerTimeDeltaPhaseThree == null ) {
            this.schedulerTimeDeltaPhaseThree = new ArrayList<>();
        }
        this.schedulerTimeDeltaPhaseThree.add( schedulerTimeDeltaPhaseThree );
    }

    public void writeRecord( String tracePath ) throws IOException {

        try ( BufferedWriter bw = new BufferedWriter( new FileWriter( tracePath ) ) ) {
            bw.write("nextflow.scheduler.trace/v1\n");
            writeValue("scheduler_files_bytes", schedulerFilesBytes, bw);
            writeValue("scheduler_files_node_bytes", schedulerFilesNodeBytes, bw);
            writeValue("scheduler_files_node_other_task_bytes", schedulerFilesNodeOtherTaskBytes, bw);
            writeValue("scheduler_files", schedulerFiles, bw);
            writeValue("scheduler_files_node", schedulerFilesNode, bw);
            writeValue("scheduler_files_node_other_task", schedulerFilesNodeOtherTask, bw);
            writeValue("scheduler_depending_task", schedulerDependingTask, bw);
            writeValue("scheduler_time_in_queue", schedulerTimeInQueue, bw);
            writeValue("scheduler_place_in_queue", schedulerPlaceInQueue, bw);
            writeValue("scheduler_location_count", schedulerLocationCount, bw);
            writeValue("scheduler_nodes_tried", schedulerNodesTried, bw);
            writeValue("scheduler_nodes_cost", schedulerNodesCost, bw);
            writeValue("scheduler_could_stop_fetching", schedulerCouldStopFetching, bw);
            writeValue("scheduler_best_cost", schedulerBestCost, bw);
            writeValue("scheduler_tried_to_schedule", schedulerTriedToSchedule, bw);
            writeValue("scheduler_nodes_to_copy_from", schedulerNodesToCopyFrom, bw);
            writeValue("scheduler_time_to_schedule", schedulerTimeToSchedule, bw);
            writeValue("scheduler_no_alignment_found", schedulerNoAlignmentFound, bw);
            writeValue("scheduler_delta_schedule_submitted", schedulerDeltaScheduleSubmitted, bw);
            writeValue("scheduler_delta_schedule_alignment", schedulerDeltaScheduleAlignment, bw);
            writeValue("scheduler_batch_id", schedulerBatchId, bw);
            writeValue("scheduler_delta_batch_start_submitted", schedulerDeltaBatchStartSubmitted, bw);
            writeValue("scheduler_delta_batch_start_received", schedulerDeltaBatchStartReceived, bw);
            writeValue("scheduler_delta_batch_closed_batch_end", schedulerDeltaBatchClosedBatchEnd, bw);
            writeValue("scheduler_delta_submitted_batch_end", schedulerDeltaSubmittedBatchEnd, bw);
            writeValue("scheduler_time_delta_phase_three", schedulerTimeDeltaPhaseThree, bw);
            writeValue("scheduler_copy_tasks", schedulerCopyTasks, bw);
        }

    }

    private <T extends Number> void writeValue( String name, T value, BufferedWriter bw ) throws IOException {
        if ( value != null ) {
            bw.write( name + '=' + value + '\n' );
        }
    }

    private void writeValue( String name, List<? extends Number> value, BufferedWriter bw ) throws IOException {
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

    public void copyTask(){
        schedulerCopyTasks++;
    }


}
