package org.codelibs.elasticsearch.taste.rest.exception;

public class InvalidParameterException extends RuntimeException {

    private static final long serialVersionUID = 1L;

    public InvalidParameterException(final String message, final Throwable cause) {
        super(message, cause);
    }

    public InvalidParameterException(final String message) {
        super(message);
    }

}
