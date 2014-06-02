package org.codelibs.elasticsearch.taste.exception;

import org.codelibs.elasticsearch.taste.TasteSystemException;

public class InvalidParameterException extends TasteSystemException {

    private static final long serialVersionUID = 1L;

    public InvalidParameterException(final String message, final Throwable cause) {
        super(message, cause);
    }

    public InvalidParameterException(final String message) {
        super(message);
    }

}
