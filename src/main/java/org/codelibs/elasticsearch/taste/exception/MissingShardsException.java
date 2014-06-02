package org.codelibs.elasticsearch.taste.exception;

import org.codelibs.elasticsearch.taste.TasteSystemException;

public class MissingShardsException extends TasteSystemException {

    private static final long serialVersionUID = 1L;

    public MissingShardsException(final String message, final Throwable cause) {
        super(message, cause);
    }

    public MissingShardsException(final String message) {
        super(message);
    }

}
