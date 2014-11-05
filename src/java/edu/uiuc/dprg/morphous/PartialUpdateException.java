package edu.uiuc.dprg.morphous;

/**
 * Created by Daniel on 10/8/14.
 */
public class PartialUpdateException extends MorphousException {

    public PartialUpdateException() {
        super();
    }

    public PartialUpdateException(String message, Throwable cause) {
        super(message, cause);
    }

    public PartialUpdateException(String message) {
        super(message);
    }

    public PartialUpdateException(Throwable cause) {
        super(cause);
    }
}
