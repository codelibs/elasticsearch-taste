package org.codelibs.elasticsearch.taste.rest.handler;

import static org.elasticsearch.rest.RestStatus.OK;

import java.util.Map;

import org.codelibs.elasticsearch.taste.rest.exception.MissingShardsException;
import org.codelibs.elasticsearch.taste.rest.exception.OperationFailedException;
import org.elasticsearch.action.ShardOperationFailedException;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.action.search.ShardSearchFailure;
import org.elasticsearch.client.Client;
import org.elasticsearch.common.logging.ESLogger;
import org.elasticsearch.common.logging.Loggers;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.json.JsonXContent;
import org.elasticsearch.rest.BytesRestResponse;
import org.elasticsearch.rest.RestChannel;
import org.elasticsearch.rest.RestRequest;

public abstract class RequestHandler {
    protected static final String ERROR_LIST = "error.list";

    protected static final String FIELD_TIMESTAMP = "field.timestamp";

    protected static final String FIELD_VALUE = "field.value";

    protected static final String FIELD_ITEM_ID = "field.item_id";

    protected static final String FIELD_USER_ID = "field.user_id";

    protected Settings settings;

    protected Client client;

    protected int maxRetryCount;

    protected final ESLogger logger;

    public RequestHandler(final Settings settings, final Client client) {
        this.settings = settings;
        this.client = client;
        maxRetryCount = settings.getAsInt("taste.rest.retry", 3);
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

    protected void sendAcknowledgedResponse(final RestRequest request,
            final RestChannel channel) {
        try {
            final XContentBuilder builder = JsonXContent.contentBuilder();
            builder.startObject();
            builder.field("acknowledged", true);
            builder.endObject();
            channel.sendResponse(new BytesRestResponse(OK, builder));
        } catch (final Exception e) {
            sendErrorResponse(request, channel, e);
        }
    }

    protected void sendErrorResponse(final RestRequest request,
            final RestChannel channel, final Throwable t) {
        try {
            channel.sendResponse(new BytesRestResponse(channel, t));
        } catch (final Exception e) {
            logger.error("Failed to send a failure response.", e);
        }
    }

    public abstract void process(final RestRequest request,
            final RestChannel channel, final Map<String, Object> requestMap,
            final Map<String, Object> paramMap, Chain chain);

    public static class Chain {
        RequestHandler[] handlers;

        int position = 0;

        public Chain(final RequestHandler[] handlers) {
            this.handlers = handlers;
        }

        public void process(final RestRequest request,
                final RestChannel channel,
                final Map<String, Object> requestMap,
                final Map<String, Object> paramMap) {
            synchronized (handlers) {
                if (position < handlers.length) {
                    final RequestHandler handler = handlers[position];
                    position++;
                    handler.process(request, channel, requestMap, paramMap,
                            this);
                }
            }
        }
    }
}
