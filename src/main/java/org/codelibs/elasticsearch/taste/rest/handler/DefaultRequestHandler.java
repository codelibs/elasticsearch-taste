package org.codelibs.elasticsearch.taste.rest.handler;

import java.util.Map;
import java.util.Random;

import org.codelibs.elasticsearch.taste.exception.MissingShardsException;
import org.codelibs.elasticsearch.taste.exception.OperationFailedException;
import org.elasticsearch.action.ShardOperationFailedException;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.action.search.ShardSearchFailure;
import org.elasticsearch.client.Client;
import org.elasticsearch.common.logging.ESLogger;
import org.elasticsearch.common.logging.Loggers;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.xcontent.ToXContent.Params;

public abstract class DefaultRequestHandler implements RequestHandler {
    protected static final String ERROR_LIST = "error.list";

    public static final String FIELD_TIMESTAMP = "field.timestamp";

    public static final String FIELD_VALUE = "field.value";

    public static final String FIELD_ITEM_ID = "field.item_id";

    public static final String FIELD_USER_ID = "field.user_id";

    protected static Random random = new Random();

    protected Settings settings;

    protected Client client;

    protected int maxRetryCount;

    protected final ESLogger logger;

    public DefaultRequestHandler(final Settings settings, final Client client) {
        this.settings = settings;
        this.client = client;
        maxRetryCount = settings.getAsInt("taste.rest.retry", 5);
        logger = Loggers.getLogger(getClass(), settings);
    }

    protected void validateRespose(final SearchResponse response) {
        final int totalShards = response.getTotalShards();
        final int successfulShards = response.getSuccessfulShards();
        if (totalShards != successfulShards) {
            throw new MissingShardsException(totalShards - successfulShards
                    + " shards are failed.");
        }
        final ShardSearchFailure[] failures = response.getShardFailures();
        if (failures.length > 0) {
            final StringBuilder buf = new StringBuilder();
            for (final ShardOperationFailedException failure : failures) {
                buf.append('\n').append(failure.toString());
            }
            throw new OperationFailedException("Search Operation Failed: "
                    + buf.toString());
        }
    }

    protected void sleep() {
        final long waitTime = random.nextInt(500) + 500;
        if (logger.isDebugEnabled()) {
            logger.debug(
                    "The search request is rejected. Waiting for {} and retrying it.",
                    waitTime);
        }
        try {
            Thread.sleep(waitTime);
        } catch (final InterruptedException e1) {
            // ignore
        }
    }

    /* (non-Javadoc)
     * @see org.codelibs.elasticsearch.taste.rest.handler.RequestHandler#execute(org.elasticsearch.common.xcontent.ToXContent.Params, org.codelibs.elasticsearch.taste.rest.handler.DefaultRequestHandler.OnErrorListener, java.util.Map, java.util.Map, org.codelibs.elasticsearch.taste.rest.handler.DefaultRequestHandler.Chain)
     */
    @Override
    public abstract void execute(final Params params,
            final RequestHandler.OnErrorListener listener,
            final Map<String, Object> requestMap,
            final Map<String, Object> paramMap, RequestHandlerChain chain);

}
