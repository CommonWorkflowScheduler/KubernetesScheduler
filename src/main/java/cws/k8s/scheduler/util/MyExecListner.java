package cws.k8s.scheduler.util;

import io.fabric8.kubernetes.client.dsl.ExecListener;
import io.fabric8.kubernetes.client.dsl.ExecWatch;

import java.io.ByteArrayOutputStream;

public interface MyExecListner extends ExecListener {

    void setExec( ExecWatch exec);
    void setError( ByteArrayOutputStream error );
    void setOut( ByteArrayOutputStream out );


}
