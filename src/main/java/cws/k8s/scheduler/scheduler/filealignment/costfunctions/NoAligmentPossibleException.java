package cws.k8s.scheduler.scheduler.filealignment.costfunctions;

public class NoAligmentPossibleException extends RuntimeException {

    public NoAligmentPossibleException(String message) {
        super(message);
    }
}
