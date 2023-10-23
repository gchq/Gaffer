package uk.gov.gchq.gaffer.graph.hook.exception;

/**
 * Generic runtime exception for errors relating to graph hooks.
 */
public class GraphHookException extends RuntimeException {

    /**
     * Constructor with basic message.
     *
     * @param message The error message.
     */
    public GraphHookException(final String message) {
        super(message);
    }

    /**
     * Constructor with message and throwable cause.
     *
     * @param message The message.
     * @param cause Throwable cause
     */
    public GraphHookException(final String message, final Throwable cause) {
        super(message, cause);
    }

}
