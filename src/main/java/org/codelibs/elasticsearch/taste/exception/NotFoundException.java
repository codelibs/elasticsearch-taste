package org.codelibs.elasticsearch.taste.exception;

import org.codelibs.elasticsearch.taste.TasteSystemException;

public class NotFoundException extends TasteSystemException {

    private static final long serialVersionUID = 1L;

    public NotFoundException(final String message, final Throwable cause) {
        super(message, cause);
    }

    public NotFoundException(final String message) {
        super(message);
    }

}
