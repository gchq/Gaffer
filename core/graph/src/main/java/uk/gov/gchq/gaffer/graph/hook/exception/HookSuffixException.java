package uk.gov.gchq.gaffer.graph.hook.exception;

/**
 * Runtime exception for issues with the graph hook suffix
 */
public class HookSuffixException extends GraphHookException {
    /**
     * Constructor with basic message.
     *
     * @param message The error message.
     */
    public HookSuffixException(final String message) {
        super(message);
    }

    /**
     * Constructor with message and throwable cause.
     *
     * @param message The message.
     * @param cause Throwable cause
     */
    public HookSuffixException(final String message, final Throwable cause) {
        super(message, cause);
    }
}
