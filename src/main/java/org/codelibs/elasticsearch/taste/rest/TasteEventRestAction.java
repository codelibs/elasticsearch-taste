package org.codelibs.elasticsearch.taste.rest;

import static org.elasticsearch.rest.RestStatus.OK;

import java.util.HashMap;
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
import org.elasticsearch.common.xcontent.ToXContent.Params;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentFactory;
import org.elasticsearch.common.xcontent.json.JsonXContent;
import org.elasticsearch.rest.BaseRestHandler;
import org.elasticsearch.rest.BytesRestResponse;
import org.elasticsearch.rest.RestChannel;
import org.elasticsearch.rest.RestController;
import org.elasticsearch.rest.RestRequest;

public class TasteEventRestAction extends BaseRestHandler {

    private UserRequestHandler userRequestHandler;

    private ItemRequestHandler itemRequestHandler;

    private PreferenceRequestHandler preferenceRequestHandler;

    @Inject
    public TasteEventRestAction(final Settings settings, final Client client,
            final RestController restController) {
        super(settings, client);

        restController.registerHandler(RestRequest.Method.POST,
                "/{index}/_taste/event", this);
        restController.registerHandler(RestRequest.Method.POST,
                "/{index}/{type}/_taste/event", this);

        userRequestHandler = new UserRequestHandler(settings, client);
        itemRequestHandler = new ItemRequestHandler(settings, client);
        preferenceRequestHandler = new PreferenceRequestHandler(settings,
                client);
    }

    @Override
    public void handleRequest(final RestRequest request,
            final RestChannel channel) {

        try {
            final Map<String, Object> requestMap = XContentFactory
                    .xContent(request.content())
                    .createParser(request.content()).mapAndClose();

            final Map<String, Object> paramMap = new HashMap<>();
            final boolean hasUser = userRequestHandler.hasUser(requestMap);
            final boolean hasItem = itemRequestHandler.hasItem(requestMap);
            final boolean hasPreference = preferenceRequestHandler
                    .hasPreference(requestMap);

            if (hasPreference) {
                final RequestHandlerChain chain = new RequestHandlerChain(
                        new RequestHandler[] { userRequestHandler,
                                itemRequestHandler, preferenceRequestHandler,
                                createAcknowledgedHandler(channel) });
                chain.execute(request, createOnErrorListener(channel),
                        requestMap, paramMap);
            } else if (hasUser) {
                final RequestHandlerChain chain = new RequestHandlerChain(
                        new RequestHandler[] { userRequestHandler,
                                createAcknowledgedHandler(channel) });
                chain.execute(request, createOnErrorListener(channel),
                        requestMap, paramMap);
            } else if (hasItem) {
                final RequestHandlerChain chain = new RequestHandlerChain(
                        new RequestHandler[] { itemRequestHandler,
                                createAcknowledgedHandler(channel) });
                chain.execute(request, createOnErrorListener(channel),
                        requestMap, paramMap);
            } else {
                throw new InvalidParameterException("No preference data.");
            }
        } catch (final Exception e) {
            createOnErrorListener(channel).onError(e);
        }

    }

    private RequestHandler createAcknowledgedHandler(final RestChannel channel) {
        return new RequestHandler() {
            @Override
            public void execute(final Params request,
                    final RequestHandler.OnErrorListener listener,
                    final Map<String, Object> requestMap,
                    final Map<String, Object> paramMap,
                    final RequestHandlerChain chain) {
                try {
                    final XContentBuilder builder = JsonXContent
                            .contentBuilder();
                    builder.startObject();
                    builder.field("acknowledged", true);
                    builder.endObject();
                    channel.sendResponse(new BytesRestResponse(OK, builder));
                } catch (final Exception e) {
                    try {
                        channel.sendResponse(new BytesRestResponse(channel, e));
                    } catch (final Exception ex) {
                        logger.error("Failed to send a failure response.", ex);
                    }
                }
            }
        };
    }

    private RequestHandler.OnErrorListener createOnErrorListener(
            final RestChannel channel) {
        return new RequestHandler.OnErrorListener() {
            @Override
            public void onError(final Throwable t) {
                try {
                    channel.sendResponse(new BytesRestResponse(channel, t));
                } catch (final Exception e) {
                    logger.error("Failed to send a failure response.", e);
                }
            }
        };
    }

}
