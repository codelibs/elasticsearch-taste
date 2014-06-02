package org.codelibs.elasticsearch.taste.exception;

import org.codelibs.elasticsearch.taste.TasteSystemException;

public class OperationFailedException extends TasteSystemException {

    private static final long serialVersionUID = 1L;

    public OperationFailedException(final String message, final Throwable cause) {
        super(message, cause);
    }

    public OperationFailedException(final String message) {
        super(message);
    }

}
