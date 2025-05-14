package cws.k8s.scheduler.publishDir;

import cws.k8s.scheduler.scheduler.Scheduler;
import cws.k8s.scheduler.util.MyExecListner;
import io.fabric8.kubernetes.api.model.Status;
import io.fabric8.kubernetes.client.dsl.ExecWatch;
import lombok.RequiredArgsConstructor;
import lombok.Setter;
import lombok.extern.slf4j.Slf4j;

import java.io.ByteArrayOutputStream;

@Slf4j
@RequiredArgsConstructor
public class PublishListener implements MyExecListner {

    @Setter
    private ExecWatch exec;
    @Setter
    private ByteArrayOutputStream out = new ByteArrayOutputStream();
    @Setter
    private ByteArrayOutputStream error = new ByteArrayOutputStream();
    private final Scheduler scheduler;
    private final String name;
    private final Runnable onFinish;

    private boolean finished = false;

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
        }
        scheduler.informResourceChange();
    }

    @Override
    public void onFailure( Throwable t, Response failureResponse ) {
        log.info( "{} failed, output: ", name, t );
        log.info( "{} Exec Output: {} ", name, out );
        log.info( "{} Exec Error Output: {} ", name,  error );
        close();
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
        onFinish.run();
        close();
    }

}
