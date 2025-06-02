package cws.k8s.scheduler.scheduler.la2.copystrategy;

import cws.k8s.scheduler.scheduler.LocationAwareSchedulerV2;
import cws.k8s.scheduler.util.CopyTask;
import cws.k8s.scheduler.util.LogCopyTask;
import cws.k8s.scheduler.util.MyExecListner;
import cws.k8s.scheduler.util.NodeTaskFilesAlignment;
import io.fabric8.kubernetes.api.model.Status;
import io.fabric8.kubernetes.client.dsl.ExecWatch;
import lombok.RequiredArgsConstructor;
import lombok.Setter;
import lombok.extern.slf4j.Slf4j;

import java.io.ByteArrayOutputStream;

@Slf4j
@RequiredArgsConstructor
public class LaListener implements MyExecListner {

    @Setter
    private ExecWatch exec;
    private final CopyTask copyTask;
    private final String name;
    @Setter
    private ByteArrayOutputStream out = new ByteArrayOutputStream();
    @Setter
    private ByteArrayOutputStream error = new ByteArrayOutputStream();
    private boolean finished = false;

    private final NodeTaskFilesAlignment nodeTaskFilesAlignment;

    private final LocationAwareSchedulerV2 scheduler;

    private final LogCopyTask logCopyTask;

    private void close() {
        //Maybe exec was not yet set
        int trial = 0;
        while( exec == null && trial < 5 ) {
            try {
                Thread.sleep( (long) (100 * Math.pow( 2, trial )) );
            } catch ( InterruptedException e ) {
                e.printStackTrace();
                Thread.currentThread().interrupt();
            }
            trial++;
        }
        if ( exec != null ) {
            exec.close();
        }
    }

    @Override
    public void onClose( int exitCode, String reason ) {
        if ( !finished ) {
            log.error( "Copy task was not finished, but closed. ExitCode: " + exitCode + " Reason: " + reason );
            scheduler.copyTaskFinished( copyTask, exitCode == 0 );
        }
    }

    @Override
    public void onFailure( Throwable t, Response failureResponse ) {
        log.info( "{} failed, output: ", name, t );
        log.info( "{} Exec Output: {} ", name, out );
        log.info( "{} Exec Error Output: {} ", name,  error );
        close();
        logCopyTask.copy( nodeTaskFilesAlignment.task.getConfig().getName(), nodeTaskFilesAlignment.node.getName(), copyTask.getInputFiles().size(), "failed" );
    }

    @Override
    public void onExit( int exitCode, Status reason ) {
        finished = true;
        if ( exitCode != 0 ) {
            log.info( "{} was finished exitCode = {}, reason = {}", name, exitCode, reason );
            log.info( "{} Exec Output: {} ", name,  out );
            log.info( "{} Exec Error Output: {} ", name, error );
        } else {
            log.info( "{} was finished successfully", name );
            log.debug( "{} Exec Output: {} ", name, out );
            log.debug( "{} Exec Error Output: {} ", name, error );
        }
        scheduler.copyTaskFinished( copyTask, exitCode == 0 );
        close();
        logCopyTask.copy( nodeTaskFilesAlignment.task.getConfig().getName(), nodeTaskFilesAlignment.node.getName(), copyTask.getInputFiles().size(), "finished(" + exitCode + ")" );
    }
}