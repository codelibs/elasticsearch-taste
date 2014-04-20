package org.codelibs.elasticsearch.taste.rest;

import static org.elasticsearch.rest.RestStatus.OK;

import java.io.IOException;
import java.security.InvalidParameterException;
import java.util.Date;
import java.util.HashMap;
import java.util.Map;

import org.codelibs.elasticsearch.taste.TasteConstants;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.get.GetResponse;
import org.elasticsearch.action.index.IndexResponse;
import org.elasticsearch.client.Client;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.joda.time.format.ISODateTimeFormat;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentFactory;
import org.elasticsearch.rest.BaseRestHandler;
import org.elasticsearch.rest.RestChannel;
import org.elasticsearch.rest.RestController;
import org.elasticsearch.rest.RestRequest;
import org.elasticsearch.rest.XContentRestResponse;
import org.elasticsearch.rest.XContentThrowableRestResponse;
import org.elasticsearch.rest.action.support.RestXContentBuilder;

public class TasteEventRestAction extends BaseRestHandler {

    @Inject
    public TasteEventRestAction(final Settings settings, final Client client,
            final RestController restController) {
        super(settings, client);

        restController.registerHandler(RestRequest.Method.POST,
                "/{index}/_taste/event", this);
        restController.registerHandler(RestRequest.Method.POST,
                "/{index}/{type}/_taste/event", this);
    }

    @Override
    public void handleRequest(final RestRequest request,
            final RestChannel channel) {

        try {
            final Map<String, Object> requestMap = XContentFactory
                    .xContent(request.content())
                    .createParser(request.content()).mapAndClose();
            final Map<String, Object> paramMap = new HashMap<>();

            processUserRequest(request, channel, requestMap, paramMap);
        } catch (final Exception e) {
            sendErrorResponse(request, channel, e);
        }

    }

    private void processUserRequest(final RestRequest request,
            final RestChannel channel, final Map<String, Object> requestMap,
            final Map<String, Object> paramMap) {
        final String index = request.param("index");
        final String userType = request.param("user_type",
                TasteConstants.USER_TYPE);

        final Map<String, Object> userMap = (Map<String, Object>) requestMap
                .get("user");
        if (userMap == null) {
            throw new InvalidParameterException("User is null.");
        }
        final Object userId = userMap.get("id");
        if (userId == null) {
            throw new InvalidParameterException("User ID is null.");
        }
        client.prepareGet(index, userType, userId.toString()).execute(
                new ActionListener<GetResponse>() {
                    @Override
                    public void onResponse(final GetResponse response) {
                        if (response.isExists()) {
                            final Map<String, Object> resposneUserMap = response
                                    .getSourceAsMap();
                            // TODO retry?
                            paramMap.put("user", resposneUserMap);
                            processItemRequest(request, channel, requestMap,
                                    paramMap);
                        } else {
                            // TODO create
                        }
                    }

                    @Override
                    public void onFailure(final Throwable t) {
                        sendErrorResponse(request, channel, t);
                    }
                });
    }

    private void processItemRequest(final RestRequest request,
            final RestChannel channel, final Map<String, Object> requestMap,
            final Map<String, Object> paramMap) {
        final String index = request.param("index");
        final String itemType = request.param("item_type",
                TasteConstants.ITEM_TYPE);

        final Map<String, Object> itemMap = (Map<String, Object>) requestMap
                .get("item");
        if (itemMap == null) {
            throw new InvalidParameterException("Item is null.");
        }
        final Object itemId = itemMap.get("id");
        if (itemId == null) {
            throw new InvalidParameterException("Item ID is null.");
        }
        client.prepareGet(index, itemType, itemId.toString()).execute(
                new ActionListener<GetResponse>() {
                    @Override
                    public void onResponse(final GetResponse response) {
                        if (response.isExists()) {
                            final Map<String, Object> responseItemMap = response
                                    .getSourceAsMap();
                            // TODO retry?
                            paramMap.put("item", responseItemMap);
                            processValueRequest(request, channel, requestMap,
                                    paramMap);
                        } else {
                            // TODO create
                        }
                    }

                    @Override
                    public void onFailure(final Throwable t) {
                        sendErrorResponse(request, channel, t);
                    }
                });
    }

    private void processValueRequest(final RestRequest request,
            final RestChannel channel, final Map<String, Object> requestMap,
            final Map<String, Object> paramMap) {
        final String index = request.param("index");
        final String type = request.param("type",
                TasteConstants.PREFERENCE_TYPE);
        final String userIdField = request.param("field.user_id",
                TasteConstants.USER_ID_FIELD);
        final String itemIdField = request.param("field.item_id",
                TasteConstants.ITEM_ID_FIELD);
        final String valueField = request.param("field.value",
                TasteConstants.VALUE_FIELD);
        final String timestampField = request.param("field.timestamp",
                TasteConstants.TIMESTAMP_FIELD);

        final Number value = (Number) requestMap.get("value");
        if (value == null) {
            throw new InvalidParameterException("value is null.");
        }

        Date timestamp;
        final Object timestampObj = requestMap.get("timestamp");
        if (timestampObj == null) {
            timestamp = new Date();
        } else if (timestampObj instanceof String) {
            timestamp = new Date(ISODateTimeFormat.dateTime().parseMillis(
                    timestampObj.toString()));
        } else if (timestampObj instanceof Date) {
            timestamp = (Date) timestampObj;
        } else {
            throw new InvalidParameterException("timestamp is invalid format: "
                    + timestampObj);
        }

        final Map<String, Object> userMap = (Map<String, Object>) paramMap
                .get("user");
        final Long userId = (Long) userMap.get(userIdField);
        paramMap.get("item");
        final Long itemId = (Long) userMap.get(itemIdField);

        final Map<String, Object> rootObj = new HashMap<>();
        rootObj.put(userIdField, userId);
        rootObj.put(itemIdField, itemId);
        rootObj.put(valueField, value);
        rootObj.put(timestampField, timestamp);
        client.prepareIndex(index, type).setSource(rootObj)
        .execute(new ActionListener<IndexResponse>() {

            @Override
            public void onResponse(final IndexResponse response) {
                try {
                    final XContentBuilder builder = RestXContentBuilder
                            .restContentBuilder(request);
                    builder.startObject();
                    builder.field(userIdField, userId);
                    builder.field(itemIdField, itemId);
                    builder.endObject();
                    channel.sendResponse(new XContentRestResponse(
                            request, OK, builder));
                } catch (final IOException e) {
                    sendErrorResponse(request, channel, e);
                }
            }

            @Override
            public void onFailure(final Throwable t) {
                sendErrorResponse(request, channel, t);
            }
        });
    }

    private void sendErrorResponse(final RestRequest request,
            final RestChannel channel, final Throwable t) {
        try {
            channel.sendResponse(new XContentThrowableRestResponse(request, t));
        } catch (final Exception e) {
            logger.error("Failed to send a failure response.", e);
        }
    }

}
