package org.codelibs.elasticsearch.taste;

public class TasteSystemException extends RuntimeException {

    private static final long serialVersionUID = 1L;

    public TasteSystemException(final String message, final Throwable cause) {
        super(message, cause);
    }

    public TasteSystemException(final String message) {
        super(message);
    }

}
