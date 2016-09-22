package org.codelibs.elasticsearch.taste.rest;

import static org.elasticsearch.rest.RestStatus.OK;

import java.util.LinkedHashMap;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;

import org.codelibs.elasticsearch.taste.exception.TasteException;
import org.codelibs.elasticsearch.taste.rest.handler.ActionHandler;
import org.codelibs.elasticsearch.taste.rest.handler.EvalItemsFromUserHandler;
import org.codelibs.elasticsearch.taste.rest.handler.GenTermValuesHandler;
import org.codelibs.elasticsearch.taste.rest.handler.ItemsFromItemHandler;
import org.codelibs.elasticsearch.taste.rest.handler.ItemsFromUserHandler;
import org.codelibs.elasticsearch.taste.rest.handler.SimilarUsersHandler;
import org.codelibs.elasticsearch.taste.service.TasteService;
import org.elasticsearch.client.Client;
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.json.JsonXContent;
import org.elasticsearch.rest.BaseRestHandler;
import org.elasticsearch.rest.BytesRestResponse;
import org.elasticsearch.rest.RestChannel;
import org.elasticsearch.rest.RestController;
import org.elasticsearch.rest.RestRequest;
import org.elasticsearch.search.lookup.SourceLookup;
import org.elasticsearch.threadpool.ThreadPool;

public class TasteActionRestAction extends BaseRestHandler {
    private static final String THREAD_NAME_PREFIX = "Taste-";

    private static final String GENERATE_TERM_VALUES = "generate_term_values";

    private static final String EVALUATE_ITEMS_FROM_USER = "evaluate_items_from_user";

    private static final String RECOMMENDED_ITEMS_FROM_ITEM = "recommended_items_from_item";

    private static final String RECOMMENDED_ITEMS_FROM_USER = "recommended_items_from_user";

    private static final String SIMILAR_USERS = "similar_users";

    private final TasteService tasteService;

    private final ThreadPool pool;

    @Inject
    public TasteActionRestAction(final Settings settings,
            final RestController restController, final Client client, final ThreadPool pool,
            final TasteService tasteService) {
        super(settings, restController, client);
        this.pool = pool;
        this.tasteService = tasteService;

        restController.registerHandler(RestRequest.Method.GET,
                "/_taste/action", this);
        restController.registerHandler(RestRequest.Method.GET,
                "/_taste/action/{name}", this);
        restController.registerHandler(RestRequest.Method.POST,
                "/_taste/action/{action}", this);
        restController.registerHandler(RestRequest.Method.DELETE,
                "/_taste/action/{name}", this);

    }

    Map<String, Thread> handlerMap = new ConcurrentHashMap<>();

    @Override
    protected void handleRequest(final RestRequest request,
            final RestChannel channel, final Client client) {
        String name;
        Map<String, Object> params;
        switch (request.method()) {
        case GET:
            name = request.param("name");
            params = new LinkedHashMap<>();
            if (name == null) {
                params.put("names", handlerMap.keySet());
            } else {
                params.put("name", name);
                params.put("found", handlerMap.containsKey(name));
            }
            sendResponse(request, channel, params, true);
            break;
        case DELETE:
            name = request.param("name");
            params = new LinkedHashMap<>();
            params.put("name", name);
            boolean acknowledged;
            if (handlerMap.containsKey(name)) {
                final Thread thread = handlerMap.remove(name);
                thread.interrupt();
                acknowledged = true;
            } else {
                acknowledged = false;
            }
            sendResponse(request, channel, params, acknowledged);
            break;
        case POST:
            final BytesReference content = request.content();
            if (content == null) {
                sendErrorResponse(channel, new TasteException(
                        "Invalid parameter. No request body."));
                break;
            }

            final String action = request.param("action");
            try {
                final Map<String, Object> sourceMap = SourceLookup
                        .sourceAsMap(content);
                if (RECOMMENDED_ITEMS_FROM_USER.equals(action)) {
                    final ItemsFromUserHandler handler = new ItemsFromUserHandler(
                            settings, sourceMap, client, pool, tasteService);
                    name = startThread(handler);
                } else if (RECOMMENDED_ITEMS_FROM_ITEM.equals(action)) {
                    final ItemsFromItemHandler handler = new ItemsFromItemHandler(
                            settings, sourceMap, client, pool, tasteService);
                    name = startThread(handler);
                } else if (SIMILAR_USERS.equals(action)) {
                    final SimilarUsersHandler handler = new SimilarUsersHandler(
                            settings, sourceMap, client, pool, tasteService);
                    name = startThread(handler);
                } else if (EVALUATE_ITEMS_FROM_USER.equals(action)) {
                    final EvalItemsFromUserHandler handler = new EvalItemsFromUserHandler(
                            settings, sourceMap, client, pool, tasteService);
                    name = startThread(handler);
                } else if (GENERATE_TERM_VALUES.equals(action)) {
                    final GenTermValuesHandler handler = new GenTermValuesHandler(
                            settings, sourceMap, client, pool);
                    name = startThread(handler);
                } else {
                    throw new TasteException("Unknown action: " + action);
                }

                params = new LinkedHashMap<>();
                params.put("name", name);
                sendResponse(request, channel, params, true);
            } catch (final Exception e) {
                sendErrorResponse(channel, e);
            }
            break;
        default:
            sendErrorResponse(channel, new TasteException("Invalid request: "
                    + request));
            break;
        }

    }

    protected String startThread(final ActionHandler handler) {
        final String name = UUID.randomUUID().toString();
        final Thread thread = new Thread(() -> {
            try {
                handler.execute();
            } catch (final Exception e) {
                logger.error("TasteThread {} is failed.", e, name);
            } finally {
                handlerMap.remove(name);
                handler.close();
            }
        }, THREAD_NAME_PREFIX + name);
        thread.start();
        handlerMap.put(name, thread);
        return name;
    }

    private void sendResponse(final RestRequest request,
            final RestChannel channel, final Map<String, Object> params,
            final boolean acknowledged) {
        try {
            final XContentBuilder builder = JsonXContent.contentBuilder();
            if (request.hasParam("pretty")) {
                builder.prettyPrint().lfAtEnd();
            }
            builder.startObject();
            builder.field("acknowledged", acknowledged);
            if (params != null) {
                for (final Map.Entry<String, Object> entry : params.entrySet()) {
                    builder.field(entry.getKey(), entry.getValue());
                }
            }
            builder.endObject();
            channel.sendResponse(new BytesRestResponse(OK, builder));
        } catch (final Exception e) {
            sendErrorResponse(channel, e);
        }
    }

    private void sendErrorResponse(final RestChannel channel, final Throwable t) {
        try {
            channel.sendResponse(new BytesRestResponse(channel, t));
        } catch (final Exception e) {
            logger.error("Failed to send a failure response.", e);
        }
    }
}
