package org.codelibs.elasticsearch.taste.exception;

public class InvalidParameterException extends TasteException {

    private static final long serialVersionUID = 1L;

    public InvalidParameterException(final String message, final Throwable cause) {
        super(message, cause);
    }

    public InvalidParameterException(final String message) {
        super(message);
    }

}
