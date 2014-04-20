package org.codelibs.elasticsearch.taste.rest.exception;

public class MissingShardsException extends RuntimeException {

    private static final long serialVersionUID = 1L;

    public MissingShardsException(final String message, final Throwable cause) {
        super(message, cause);
    }

    public MissingShardsException(final String message) {
        super(message);
    }

}
