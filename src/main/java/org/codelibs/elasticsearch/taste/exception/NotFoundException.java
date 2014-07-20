package org.codelibs.elasticsearch.taste.exception;

public class NotFoundException extends TasteException {

    private static final long serialVersionUID = 1L;

    public NotFoundException(final String message, final Throwable cause) {
        super(message, cause);
    }

    public NotFoundException(final String message) {
        super(message);
    }

}
