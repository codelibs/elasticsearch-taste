package org.codelibs.elasticsearch.taste.exception;

import org.elasticsearch.common.logging.ESLogger;
import org.elasticsearch.common.logging.Loggers;

public class TasteException extends RuntimeException {

    private static final long serialVersionUID = 1L;

    private static final ESLogger logger = Loggers
            .getLogger(TasteException.class);

    public TasteException(final Throwable cause) {
        super(cause);
        rethrowInterruptedException("Interrupted.", cause);
    }

    public TasteException(final String message, final Throwable cause) {
        super(message, cause);
        rethrowInterruptedException(message, cause);
    }

    public TasteException(final String message) {
        super(message);
    }

    private void rethrowInterruptedException(final String message,
            final Throwable cause) {
        Throwable t = cause;
        while (t != null) {
            if (t instanceof InterruptedException) {
                if (logger.isDebugEnabled()) {
                    logger.debug(message, t);
                }
                Thread.currentThread().interrupt();
                break;
            }
            t = cause.getCause();
        }
    }

}
