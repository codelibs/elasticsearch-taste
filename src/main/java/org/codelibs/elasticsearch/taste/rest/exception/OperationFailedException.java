package org.codelibs.elasticsearch.taste.rest.exception;

public class OperationFailedException extends RuntimeException {

    private static final long serialVersionUID = 1L;

    public OperationFailedException(final String message, final Throwable cause) {
        super(message, cause);
    }

    public OperationFailedException(final String message) {
        super(message);
    }

}
