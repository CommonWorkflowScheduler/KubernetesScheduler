package cws.k8s.scheduler.util;

public enum PodPhase {

    PENDING( false ),
    RUNNING( false ),
    SUCCEEDED( true ),
    FAILED( true ),
    UNKNOWN( false );

    private final boolean finished;

    PodPhase(boolean finished ){
        this.finished = finished;
    }

    public boolean isFinished(){
        return finished;
    }

    public static PodPhase get(String name){
        switch ( name.toUpperCase() ) {
            case "PENDING": return PENDING;
            case "RUNNING": return RUNNING;
            case "SUCCEEDED": return SUCCEEDED;
            case "FAILED": return FAILED;
            case "UNKNOWN": return UNKNOWN;
            default: throw new IllegalArgumentException( "No enum with name: " + name );
        }
    }

}
