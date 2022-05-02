package fonda.scheduler.model.tracing;

import lombok.Getter;
import lombok.Setter;

import java.io.BufferedWriter;
import java.io.FileWriter;
import java.io.IOException;


public class TraceRecord {

    @Getter
    @Setter
    private Long schedulerFilesBytes = null;

    @Getter
    @Setter
    private Long schedulerFilesNodeBytes = null;

    @Getter
    @Setter
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
    private long schedulerTimeInQueue = -1;

    @Getter
    @Setter
    private int schedulerPlaceInQueue = -1;

    @Getter
    @Setter
    private int schedulerLocationCount = -1;

    private int schedulerTriedToSchedule = 0;

    @Getter
    @Setter
    private Integer schedulerNodesToCopyFrom = null;

    @Getter
    @Setter
    private Integer schedulerTimeToSchedule = null;

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
            writeValue("scheduler_tried_to_schedule", schedulerTriedToSchedule, bw);
            writeValue("scheduler_nodes_to_copy_from", schedulerNodesToCopyFrom, bw);
            writeValue("scheduler_time_to_schedule", schedulerTimeToSchedule, bw);
        }

    }

    private void writeValue( String name, Long value, BufferedWriter bw ) throws IOException {
        if ( value != null ) {
            bw.write( name + '=' + value + '\n' );
        }
    }

    private void writeValue( String name, Integer value, BufferedWriter bw ) throws IOException {
        if ( value != null ) {
            bw.write( name + '=' + value + '\n' );
        }
    }

    public void tryToSchedule(){
        schedulerTriedToSchedule++;
    }


}
