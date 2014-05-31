package org.codelibs.elasticsearch.taste.rest;

import java.util.HashMap;
import java.util.Map;

import org.codelibs.elasticsearch.taste.rest.exception.InvalidParameterException;
import org.codelibs.elasticsearch.taste.rest.handler.ItemRequestHandler;
import org.codelibs.elasticsearch.taste.rest.handler.PreferenceRequestHandler;
import org.codelibs.elasticsearch.taste.rest.handler.RequestHandler;
import org.codelibs.elasticsearch.taste.rest.handler.RequestHandler.Chain;
import org.codelibs.elasticsearch.taste.rest.handler.UserRequestHandler;
import org.elasticsearch.client.Client;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.xcontent.XContentFactory;
import org.elasticsearch.rest.BaseRestHandler;
import org.elasticsearch.rest.BytesRestResponse;
import org.elasticsearch.rest.RestChannel;
import org.elasticsearch.rest.RestController;
import org.elasticsearch.rest.RestRequest;

public class TasteEventRestAction extends BaseRestHandler {

    private UserRequestHandler userRequestHandler;

    private ItemRequestHandler itemRequestHandler;

    private PreferenceRequestHandler preferenceRequestHandler;

    private RequestHandler acknowledgedHandler;

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
        acknowledgedHandler = new RequestHandler(settings, client) {
            @Override
            public void process(final RestRequest request,
                    final RestChannel channel,
                    final Map<String, Object> requestMap,
                    final Map<String, Object> paramMap, final Chain chain) {
                sendAcknowledgedResponse(request, channel);
            }
        };
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
                final Chain chain = new Chain(new RequestHandler[] {
                        userRequestHandler, itemRequestHandler,
                        preferenceRequestHandler, acknowledgedHandler });
                chain.process(request, channel, requestMap, paramMap);
            } else if (hasUser) {
                final Chain chain = new Chain(new RequestHandler[] {
                        userRequestHandler, acknowledgedHandler });
                chain.process(request, channel, requestMap, paramMap);
            } else if (hasItem) {
                final Chain chain = new Chain(new RequestHandler[] {
                        itemRequestHandler, acknowledgedHandler });
                chain.process(request, channel, requestMap, paramMap);
            } else {
                throw new InvalidParameterException("No preference data.");
            }
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
}
