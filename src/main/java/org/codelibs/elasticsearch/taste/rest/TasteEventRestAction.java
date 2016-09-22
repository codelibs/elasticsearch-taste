package org.codelibs.elasticsearch.taste.rest;

import static org.elasticsearch.rest.RestStatus.OK;

import java.util.Arrays;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;

import org.codelibs.elasticsearch.taste.exception.InvalidParameterException;
import org.codelibs.elasticsearch.taste.rest.handler.ItemRequestHandler;
import org.codelibs.elasticsearch.taste.rest.handler.PreferenceRequestHandler;
import org.codelibs.elasticsearch.taste.rest.handler.RequestHandler;
import org.codelibs.elasticsearch.taste.rest.handler.RequestHandlerChain;
import org.codelibs.elasticsearch.taste.rest.handler.UserRequestHandler;
import org.elasticsearch.client.Client;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentFactory;
import org.elasticsearch.common.xcontent.json.JsonXContent;
import org.elasticsearch.rest.BaseRestHandler;
import org.elasticsearch.rest.BytesRestResponse;
import org.elasticsearch.rest.RestChannel;
import org.elasticsearch.rest.RestController;
import org.elasticsearch.rest.RestRequest;
import org.elasticsearch.threadpool.ThreadPool;

public class TasteEventRestAction extends BaseRestHandler {
    private final UserRequestHandler userRequestHandler;

    private final ItemRequestHandler itemRequestHandler;

    private final PreferenceRequestHandler preferenceRequestHandler;

    private final ThreadPool pool;

    @Inject
    public TasteEventRestAction(final Settings settings, final Client client,
                                final RestController restController, final ThreadPool pool) {
        super(settings, restController, client);
        this.pool = pool;

        restController.registerHandler(RestRequest.Method.POST,
                "/{index}/_taste/event", this);
        restController.registerHandler(RestRequest.Method.POST,
                "/{index}/{type}/_taste/event", this);

        userRequestHandler = new UserRequestHandler(settings, client, pool);
        itemRequestHandler = new ItemRequestHandler(settings, client, pool);
        preferenceRequestHandler = new PreferenceRequestHandler(settings,
                client, pool);
    }

    @Override
    protected void handleRequest(final RestRequest request,
                                 final RestChannel channel, final Client client) {
        final String[] data = request.content().toUtf8()
                .split("\r\n|[\n\r\u2028\u2029\u0085]");
        final Iterator<String> itr = Arrays.asList(data).iterator();

        pool.generic().execute(()->execute(request, channel, itr));
    }

    private void execute(final RestRequest request, final RestChannel channel, final Iterator<String> itr) {
        try {
            if (itr.hasNext()) {
                final String aData = itr.next();
                final Map<String, Object> requestMap = XContentFactory
                        .xContent(aData)
                        .createParser(aData).map();

                final Map<String, Object> paramMap = new HashMap<>();
                final boolean hasUser = userRequestHandler.hasUser(requestMap);
                final boolean hasItem = itemRequestHandler.hasItem(requestMap);
                final boolean hasPreference = preferenceRequestHandler
                        .hasPreference(requestMap);
                final RequestHandler continueExecuteHandler = (req, listener, reqMap, parMap, c) ->
                    execute(request, channel, itr);

                if (hasPreference) {
                    final RequestHandlerChain chain = new RequestHandlerChain(
                                    new RequestHandler[]{userRequestHandler, itemRequestHandler,
                                            preferenceRequestHandler, continueExecuteHandler});
                    chain.execute(request, createOnErrorListener(channel),
                            requestMap, paramMap);
                } else if (hasUser) {
                    final RequestHandlerChain chain = new RequestHandlerChain(
                                    new RequestHandler[]{userRequestHandler, continueExecuteHandler});
                    chain.execute(request, createOnErrorListener(channel),
                            requestMap, paramMap);
                } else if (hasItem) {
                    final RequestHandlerChain chain = new RequestHandlerChain(
                            new RequestHandler[]{itemRequestHandler, continueExecuteHandler});
                    chain.execute(request, createOnErrorListener(channel),
                            requestMap, paramMap);
                } else {
                    throw new InvalidParameterException("No preference data.");
                }
            } else {
                final XContentBuilder builder = JsonXContent.contentBuilder();
                final String pretty = request.param("pretty");
                if (pretty != null && !"false".equalsIgnoreCase(pretty)) {
                    builder.prettyPrint().lfAtEnd();
                }
                builder.startObject();
                builder.field("acknowledged", true);
                builder.endObject();
                channel.sendResponse(new BytesRestResponse(OK, builder));
            }
        } catch (final Exception e) {
            try {
                channel.sendResponse(new BytesRestResponse(channel, e));
            } catch (final Exception ex) {
                logger.error("Failed to send a failure response.", ex);
            }
        }
    }

    private RequestHandler.OnErrorListener createOnErrorListener(
            final RestChannel channel) {
        return t -> {
            try {
                channel.sendResponse(new BytesRestResponse(channel, t));
            } catch (final Exception e) {
                logger.error("Failed to send a failure response.", e);
            }
        };
    }
}
