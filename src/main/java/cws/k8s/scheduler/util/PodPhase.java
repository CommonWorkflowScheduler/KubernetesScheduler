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
        return switch ( name.toUpperCase() ) {
            case "PENDING" -> PENDING;
            case "RUNNING" -> RUNNING;
            case "SUCCEEDED" -> SUCCEEDED;
            case "FAILED" -> FAILED;
            case "UNKNOWN" -> UNKNOWN;
            default -> throw new IllegalArgumentException( "No enum with name: " + name );
        };
    }

}
