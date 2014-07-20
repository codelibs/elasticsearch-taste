package org.codelibs.elasticsearch.taste.exception;

public class TasteException extends RuntimeException {

    private static final long serialVersionUID = 1L;

    public TasteException(final Throwable cause) {
        super(cause);
    }

    public TasteException(final String message, final Throwable cause) {
        super(message, cause);
    }

    public TasteException(final String message) {
        super(message);
    }

}
