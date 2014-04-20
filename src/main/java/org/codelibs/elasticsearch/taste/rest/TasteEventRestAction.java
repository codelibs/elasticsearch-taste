package org.codelibs.elasticsearch.taste.rest;

import static org.elasticsearch.rest.RestStatus.OK;

import java.io.IOException;
import java.security.InvalidParameterException;
import java.util.Date;
import java.util.HashMap;
import java.util.Map;

import org.codelibs.elasticsearch.taste.TasteConstants;
import org.codelibs.elasticsearch.taste.rest.exception.MissingShardsException;
import org.codelibs.elasticsearch.taste.rest.exception.OperationFailedException;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.ShardOperationFailedException;
import org.elasticsearch.action.index.IndexRequest.OpType;
import org.elasticsearch.action.index.IndexResponse;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.action.search.ShardSearchFailure;
import org.elasticsearch.client.Client;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.joda.time.format.ISODateTimeFormat;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentFactory;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.rest.BaseRestHandler;
import org.elasticsearch.rest.RestChannel;
import org.elasticsearch.rest.RestController;
import org.elasticsearch.rest.RestRequest;
import org.elasticsearch.rest.XContentRestResponse;
import org.elasticsearch.rest.XContentThrowableRestResponse;
import org.elasticsearch.rest.action.support.RestXContentBuilder;
import org.elasticsearch.search.SearchHit;
import org.elasticsearch.search.SearchHitField;
import org.elasticsearch.search.SearchHits;
import org.elasticsearch.search.sort.SortOrder;

public class TasteEventRestAction extends BaseRestHandler {

    private static final String FIELD_TIMESTAMP = "field.timestamp";

    private static final String FIELD_VALUE = "field.value";

    private static final String FIELD_ITEM_ID = "field.item_id";

    private static final String FIELD_USER_ID = "field.user_id";

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

            handleUserRequest(request, channel, requestMap, paramMap);
        } catch (final Exception e) {
            sendErrorResponse(request, channel, e);
        }

    }

    private void handleUserRequest(final RestRequest request,
            final RestChannel channel, final Map<String, Object> requestMap,
            final Map<String, Object> paramMap) {
        final String index = request.param("index");
        final String userType = request.param("user_type",
                TasteConstants.USER_TYPE);
        final String userIdField = request.param(FIELD_USER_ID,
                TasteConstants.USER_ID_FIELD);
        final String timestampField = request.param(FIELD_TIMESTAMP,
                TasteConstants.TIMESTAMP_FIELD);

        @SuppressWarnings("unchecked")
        final Map<String, Object> userMap = (Map<String, Object>) requestMap
                .get("user");
        if (userMap == null) {
            throw new InvalidParameterException("User is null.");
        }
        final Object id = userMap.get("id");
        if (id == null) {
            throw new InvalidParameterException("User ID is null.");
        }
        client.prepareSearch(index).setTypes(userType)
                .setQuery(QueryBuilders.termQuery("id", id))
                .addField(userIdField).addSort(timestampField, SortOrder.DESC)
                .setSize(1).execute(new ActionListener<SearchResponse>() {

                    @Override
                    public void onResponse(SearchResponse response) {
                        try {
                            validateRespose(response);

                            SearchHits hits = response.getHits();
                            if (hits.getTotalHits() == 0) {
                                handleUserCreation(request, channel,
                                        requestMap, paramMap, userMap, index,
                                        userType, userIdField);
                            } else {
                                SearchHit[] searchHits = hits.getHits();
                                SearchHitField field = searchHits[0]
                                        .getFields().get(userIdField);
                                if (field != null) {
                                    Long userId = field.getValue();
                                    if (userId != null) {
                                        paramMap.put(userIdField, userId);
                                        handleItemRequest(request, channel,
                                                requestMap, paramMap);
                                        return;
                                    }
                                }
                                throw new OperationFailedException(
                                        "User does not have " + userIdField
                                                + ": " + searchHits[0]);
                            }
                        } catch (Exception e) {
                            onFailure(e);
                        }
                    }

                    @Override
                    public void onFailure(Throwable t) {
                        sendErrorResponse(request, channel, t);
                    }
                });
    }

    private void handleUserCreation(final RestRequest request,
            final RestChannel channel, final Map<String, Object> requestMap,
            final Map<String, Object> paramMap,
            final Map<String, Object> userMap, final String index,
            final String type, final String userIdField) {
        client.prepareSearch(index).setTypes(type)
                .setQuery(QueryBuilders.matchAllQuery()).addField(userIdField)
                .addSort(userIdField, SortOrder.DESC).setSize(1)
                .execute(new ActionListener<SearchResponse>() {
                    @Override
                    public void onResponse(SearchResponse response) {
                        try {
                            validateRespose(response);

                            Long currentId = null;
                            SearchHits hits = response.getHits();
                            if (hits.getTotalHits() != 0) {
                                SearchHit[] searchHits = hits.getHits();
                                SearchHitField field = searchHits[0]
                                        .getFields().get(userIdField);
                                if (field != null) {
                                    currentId = field.getValue();
                                }
                            }
                            final Long userId;
                            if (currentId == null) {
                                userId = Long.valueOf(1);
                            } else {
                                userId = Long.valueOf(currentId.longValue() + 1);
                            }
                            client.prepareIndex(index, type, userId.toString())
                                    .setSource(userMap)
                                    .setRefresh(true)
                                    .setOpType(OpType.CREATE)
                                    .execute(
                                            new ActionListener<IndexResponse>() {

                                                @Override
                                                public void onResponse(
                                                        IndexResponse response) {
                                                    if (response.isCreated()) {
                                                        paramMap.put(
                                                                userIdField,
                                                                userId);
                                                        handleItemRequest(
                                                                request,
                                                                channel,
                                                                requestMap,
                                                                paramMap);
                                                    } else {
                                                        onFailure(new OperationFailedException(
                                                                "Failed to create "
                                                                        + userMap));
                                                    }
                                                }

                                                @Override
                                                public void onFailure(
                                                        Throwable t) {
                                                    sendErrorResponse(request,
                                                            channel, t);
                                                }
                                            });
                        } catch (Exception e) {
                            sendErrorResponse(request, channel, e);
                        }
                    }

                    @Override
                    public void onFailure(Throwable t) {
                        sendErrorResponse(request, channel, t);
                    }
                });
    }

    private void handleItemRequest(final RestRequest request,
            final RestChannel channel, final Map<String, Object> requestMap,
            final Map<String, Object> paramMap) {
        final String index = request.param("index");
        final String itemType = request.param("item_type",
                TasteConstants.ITEM_TYPE);
        final String itemIdField = request.param(FIELD_ITEM_ID,
                TasteConstants.ITEM_ID_FIELD);
        final String timestampField = request.param(FIELD_TIMESTAMP,
                TasteConstants.TIMESTAMP_FIELD);

        @SuppressWarnings("unchecked")
        final Map<String, Object> itemMap = (Map<String, Object>) requestMap
                .get("item");
        if (itemMap == null) {
            throw new InvalidParameterException("Item is null.");
        }
        final Object id = itemMap.get("id");
        if (id == null) {
            throw new InvalidParameterException("Item ID is null.");
        }
        client.prepareSearch(index).setTypes(itemType)
                .setQuery(QueryBuilders.termQuery("id", id))
                .addField(itemIdField).addSort(timestampField, SortOrder.DESC)
                .setSize(1).execute(new ActionListener<SearchResponse>() {

                    @Override
                    public void onResponse(SearchResponse response) {
                        try {
                            validateRespose(response);

                            SearchHits hits = response.getHits();
                            if (hits.getTotalHits() == 0) {
                                handleItemCreation(request, channel,
                                        requestMap, paramMap, itemMap, index,
                                        itemType, itemIdField);
                            } else {
                                SearchHit[] searchHits = hits.getHits();
                                SearchHitField field = searchHits[0]
                                        .getFields().get(itemIdField);
                                if (field != null) {
                                    Long itemId = field.getValue();
                                    if (itemId != null) {
                                        paramMap.put(itemIdField, itemId);
                                        handleValueRequest(request, channel,
                                                requestMap, paramMap);
                                        return;
                                    }
                                }
                                throw new OperationFailedException(
                                        "Item does not have " + itemIdField
                                                + ": " + searchHits[0]);
                            }
                        } catch (Exception e) {
                            onFailure(e);
                        }
                    }

                    @Override
                    public void onFailure(Throwable t) {
                        sendErrorResponse(request, channel, t);
                    }
                });
    }

    private void handleItemCreation(final RestRequest request,
            final RestChannel channel, final Map<String, Object> requestMap,
            final Map<String, Object> paramMap,
            final Map<String, Object> itemMap, final String index,
            final String type, final String itemIdField) {
        client.prepareSearch(index).setTypes(type)
                .setQuery(QueryBuilders.matchAllQuery()).addField(itemIdField)
                .addSort(itemIdField, SortOrder.DESC).setSize(1)
                .execute(new ActionListener<SearchResponse>() {
                    @Override
                    public void onResponse(SearchResponse response) {
                        try {
                            validateRespose(response);

                            Long currentId = null;
                            SearchHits hits = response.getHits();
                            if (hits.getTotalHits() != 0) {
                                SearchHit[] searchHits = hits.getHits();
                                SearchHitField field = searchHits[0]
                                        .getFields().get(itemIdField);
                                if (field != null) {
                                    currentId = field.getValue();
                                }
                            }
                            final Long itemId;
                            if (currentId == null) {
                                itemId = Long.valueOf(1);
                            } else {
                                itemId = Long.valueOf(currentId.longValue() + 1);
                            }
                            client.prepareIndex(index, type, itemId.toString())
                                    .setSource(itemMap)
                                    .setRefresh(true)
                                    .setOpType(OpType.CREATE)
                                    .execute(
                                            new ActionListener<IndexResponse>() {

                                                @Override
                                                public void onResponse(
                                                        IndexResponse response) {
                                                    if (response.isCreated()) {
                                                        paramMap.put(
                                                                itemIdField,
                                                                itemId);
                                                        handleValueRequest(
                                                                request,
                                                                channel,
                                                                requestMap,
                                                                paramMap);
                                                    } else {
                                                        onFailure(new OperationFailedException(
                                                                "Failed to create "
                                                                        + itemMap));
                                                    }
                                                }

                                                @Override
                                                public void onFailure(
                                                        Throwable t) {
                                                    sendErrorResponse(request,
                                                            channel, t);
                                                }
                                            });
                        } catch (Exception e) {
                            sendErrorResponse(request, channel, e);
                        }
                    }

                    @Override
                    public void onFailure(Throwable t) {
                        sendErrorResponse(request, channel, t);
                    }
                });
    }

    private void handleValueRequest(final RestRequest request,
            final RestChannel channel, final Map<String, Object> requestMap,
            final Map<String, Object> paramMap) {
        final String index = request.param("index");
        final String type = request.param("type",
                TasteConstants.PREFERENCE_TYPE);
        final String userIdField = request.param(FIELD_USER_ID,
                TasteConstants.USER_ID_FIELD);
        final String itemIdField = request.param(FIELD_ITEM_ID,
                TasteConstants.ITEM_ID_FIELD);
        final String valueField = request.param(FIELD_VALUE,
                TasteConstants.VALUE_FIELD);
        final String timestampField = request.param(FIELD_TIMESTAMP,
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

        final Long userId = (Long) paramMap.get(userIdField);
        final Long itemId = (Long) paramMap.get(itemIdField);

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

    private void validateRespose(SearchResponse response) {
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

    private void sendErrorResponse(final RestRequest request,
            final RestChannel channel, final Throwable t) {
        try {
            channel.sendResponse(new XContentThrowableRestResponse(request, t));
        } catch (final Exception e) {
            logger.error("Failed to send a failure response.", e);
        }
    }

}
