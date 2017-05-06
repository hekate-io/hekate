package io.hekate.core.resource;

import io.hekate.core.HekateException;

/**
 * Signals a resource loading error.
 *
 * @see ResourceService#load(String)
 */
public class ResourceLoadingException extends HekateException {
    private static final long serialVersionUID = 1;

    /**
     * Constructs new instance with the specified error message.
     *
     * @param message Error message.
     */
    public ResourceLoadingException(String message) {
        super(message);
    }

    /**
     * Constructs new instance with the specified error message and cause.
     *
     * @param message Error message.
     * @param cause Cause.
     */
    public ResourceLoadingException(String message, Throwable cause) {
        super(message, cause);
    }
}
