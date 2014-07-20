package org.codelibs.elasticsearch.taste.exception;

public class OperationFailedException extends TasteException {

    private static final long serialVersionUID = 1L;

    public OperationFailedException(final String message, final Throwable cause) {
        super(message, cause);
    }

    public OperationFailedException(final String message) {
        super(message);
    }

}
