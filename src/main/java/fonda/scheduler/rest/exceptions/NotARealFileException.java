package fonda.scheduler.rest.exceptions;

public class NotARealFileException extends Exception {

    public NotARealFileException() {
        super( "Not a real file" );
    }
}
