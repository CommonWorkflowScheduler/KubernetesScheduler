package cws.k8s.scheduler.client;

public class CannotPatchException extends RuntimeException {

    public CannotPatchException(String message) {
        super(message);
    }

}
