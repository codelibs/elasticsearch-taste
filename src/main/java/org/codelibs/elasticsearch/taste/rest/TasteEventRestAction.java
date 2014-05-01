package org.codelibs.elasticsearch.taste.rest;

import static org.elasticsearch.rest.RestStatus.OK;

import java.io.IOException;
import java.security.InvalidParameterException;
import java.util.ArrayList;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.codelibs.elasticsearch.taste.TasteConstants;
import org.codelibs.elasticsearch.taste.rest.exception.MissingShardsException;
import org.codelibs.elasticsearch.taste.rest.exception.OperationFailedException;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.ShardOperationFailedException;
import org.elasticsearch.action.admin.indices.create.CreateIndexResponse;
import org.elasticsearch.action.admin.indices.exists.indices.IndicesExistsResponse;
import org.elasticsearch.action.admin.indices.mapping.put.PutMappingResponse;
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

    private static final String ERROR_LIST = "error.list";

    private static final String FIELD_TIMESTAMP = "field.timestamp";

    private static final String FIELD_VALUE = "field.value";

    private static final String FIELD_ITEM_ID = "field.item_id";

    private static final String FIELD_USER_ID = "field.user_id";

    private int maxRetryCount;

    @Inject
    public TasteEventRestAction(final Settings settings, final Client client,
            final RestController restController) {
        super(settings, client);

        maxRetryCount = settings.getAsInt("taste.rest.retry", 3);

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
        final String index = request
                .param("user_index", request.param("index"));
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
                    public void onResponse(final SearchResponse response) {
                        try {
                            validateRespose(response);
                            final String updateType = request.param("update");

                            final SearchHits hits = response.getHits();
                            if (hits.getTotalHits() == 0) {
                                handleUserCreation(request, channel,
                                        requestMap, paramMap, userMap, index,
                                        userType, userIdField, timestampField);
                            } else {
                                final SearchHit[] searchHits = hits.getHits();
                                final SearchHitField field = searchHits[0]
                                        .getFields().get(userIdField);
                                if (field != null) {
                                    final Number userId = field.getValue();
                                    if (userId != null) {
                                        if ("all".equals(updateType)
                                                || "user".equals(updateType)) {
                                            handleUserUpdate(request, channel,
                                                    requestMap, paramMap,
                                                    userMap, index, userType,
                                                    userIdField,
                                                    timestampField,
                                                    userId.longValue(),
                                                    OpType.INDEX);

                                        } else {
                                            paramMap.put(userIdField,
                                                    userId.longValue());
                                            handleItemRequest(request, channel,
                                                    requestMap, paramMap);
                                        }
                                        return;
                                    }
                                }
                                throw new OperationFailedException(
                                        "User does not have " + userIdField
                                                + ": " + searchHits[0]);
                            }
                        } catch (final Exception e) {
                            onFailure(e);
                        }
                    }

                    @Override
                    public void onFailure(final Throwable t) {
                        @SuppressWarnings("unchecked")
                        List<Throwable> errorList = (List<Throwable>) paramMap
                                .get(ERROR_LIST);
                        if (errorList == null) {
                            errorList = new ArrayList<>();
                            paramMap.put(ERROR_LIST, errorList);
                        }
                        if (errorList.size() >= maxRetryCount) {
                            sendErrorResponse(request, channel, t);
                        } else {
                            errorList.add(t);
                            handleUserIndexCreation(request, channel,
                                    requestMap, paramMap);
                        }
                    }
                });
    }

    private void handleUserIndexCreation(final RestRequest request,
            final RestChannel channel, final Map<String, Object> requestMap,
            final Map<String, Object> paramMap) {
        final String index = request
                .param("user_index", request.param("index"));

        client.admin().indices().prepareExists(index)
                .execute(new ActionListener<IndicesExistsResponse>() {

                    @Override
                    public void onResponse(
                            final IndicesExistsResponse indicesExistsResponse) {
                        if (indicesExistsResponse.isExists()) {
                            handleUserMappingCreation(request, channel,
                                    requestMap, paramMap);
                        } else {
                            client.admin()
                                    .indices()
                                    .prepareCreate(index)
                                    .execute(
                                            new ActionListener<CreateIndexResponse>() {

                                                @Override
                                                public void onResponse(
                                                        final CreateIndexResponse createIndexResponse) {
                                                    if (createIndexResponse
                                                            .isAcknowledged()) {
                                                        handleUserMappingCreation(
                                                                request,
                                                                channel,
                                                                requestMap,
                                                                paramMap);
                                                    } else {
                                                        onFailure(new OperationFailedException(
                                                                "Failed to create "
                                                                        + index));
                                                    }
                                                }

                                                @Override
                                                public void onFailure(
                                                        final Throwable t) {
                                                    sendErrorResponse(request,
                                                            channel, t);
                                                }
                                            });
                        }
                    }

                    @Override
                    public void onFailure(final Throwable t) {
                        sendErrorResponse(request, channel, t);
                    }
                });
    }

    private void handleUserMappingCreation(final RestRequest request,
            final RestChannel channel, final Map<String, Object> requestMap,
            final Map<String, Object> paramMap) {
        final String index = request
                .param("user_index", request.param("index"));
        final String type = request
                .param("user_type", TasteConstants.USER_TYPE);
        final String userIdField = request.param(FIELD_USER_ID,
                TasteConstants.USER_ID_FIELD);
        final String timestampField = request.param(FIELD_TIMESTAMP,
                TasteConstants.TIMESTAMP_FIELD);

        try {
            final XContentBuilder builder = XContentFactory.jsonBuilder()//
                    .startObject()//
                    .startObject(type)//
                    .startObject("properties")//

                    // @timestamp
                    .startObject(timestampField)//
                    .field("type", "date")//
                    .field("format", "dateOptionalTime")//
                    .endObject()//

                    // user_id
                    .startObject(userIdField)//
                    .field("type", "long")//
                    .endObject()//

                    // id
                    .startObject("id")//
                    .field("type", "string")//
                    .field("index", "not_analyzed")//
                    .endObject()//

                    .endObject()//
                    .endObject()//
                    .endObject();

            client.admin().indices().preparePutMapping(index).setType(type)
                    .setSource(builder)
                    .execute(new ActionListener<PutMappingResponse>() {

                        @Override
                        public void onResponse(
                                final PutMappingResponse queueMappingResponse) {
                            if (queueMappingResponse.isAcknowledged()) {
                                handleUserRequest(request, channel, requestMap,
                                        paramMap);
                            } else {
                                onFailure(new OperationFailedException(
                                        "Failed to create mapping for " + index
                                                + "/" + type));
                            }
                        }

                        @Override
                        public void onFailure(final Throwable t) {
                            sendErrorResponse(request, channel, t);
                        }
                    });
        } catch (final Exception e) {
            sendErrorResponse(request, channel, e);
        }
    }

    private void handleUserCreation(final RestRequest request,
            final RestChannel channel, final Map<String, Object> requestMap,
            final Map<String, Object> paramMap,
            final Map<String, Object> userMap, final String index,
            final String type, final String userIdField,
            final String timestampField) {
        client.prepareSearch(index).setTypes(type)
                .setQuery(QueryBuilders.matchAllQuery()).addField(userIdField)
                .addSort(userIdField, SortOrder.DESC).setSize(1)
                .execute(new ActionListener<SearchResponse>() {
                    @Override
                    public void onResponse(final SearchResponse response) {
                        try {
                            validateRespose(response);

                            Number currentId = null;
                            final SearchHits hits = response.getHits();
                            if (hits.getTotalHits() != 0) {
                                final SearchHit[] searchHits = hits.getHits();
                                final SearchHitField field = searchHits[0]
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
                            handleUserUpdate(request, channel, requestMap,
                                    paramMap, userMap, index, type,
                                    userIdField, timestampField, userId,
                                    OpType.CREATE);
                        } catch (final Exception e) {
                            sendErrorResponse(request, channel, e);
                        }
                    }

                    @Override
                    public void onFailure(final Throwable t) {
                        sendErrorResponse(request, channel, t);
                    }
                });
    }

    private void handleUserUpdate(final RestRequest request,
            final RestChannel channel, final Map<String, Object> requestMap,
            final Map<String, Object> paramMap,
            final Map<String, Object> userMap, final String index,
            final String type, final String userIdField,
            final String timestampField, final Long userId, final OpType opType) {
        userMap.put(userIdField, userId);
        userMap.put(timestampField, new Date());
        client.prepareIndex(index, type, userId.toString()).setSource(userMap)
                .setRefresh(true).setOpType(opType)
                .execute(new ActionListener<IndexResponse>() {

                    @Override
                    public void onResponse(final IndexResponse response) {
                        paramMap.put(userIdField, userId);
                        handleItemRequest(request, channel, requestMap,
                                paramMap);
                    }

                    @Override
                    public void onFailure(final Throwable t) {
                        sendErrorResponse(request, channel, t);
                    }
                });
    }

    private void handleItemRequest(final RestRequest request,
            final RestChannel channel, final Map<String, Object> requestMap,
            final Map<String, Object> paramMap) {
        final String index = request
                .param("item_index", request.param("index"));
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
                    public void onResponse(final SearchResponse response) {
                        try {
                            validateRespose(response);
                            final String updateType = request.param("update");

                            final SearchHits hits = response.getHits();
                            if (hits.getTotalHits() == 0) {
                                handleItemCreation(request, channel,
                                        requestMap, paramMap, itemMap, index,
                                        itemType, itemIdField, timestampField);
                            } else {
                                final SearchHit[] searchHits = hits.getHits();
                                final SearchHitField field = searchHits[0]
                                        .getFields().get(itemIdField);
                                if (field != null) {
                                    final Number itemId = field.getValue();
                                    if (itemId != null) {
                                        if ("all".equals(updateType)
                                                || "item".equals(updateType)) {
                                            handleItemUpdate(request, channel,
                                                    requestMap, paramMap,
                                                    itemMap, index, itemType,
                                                    itemIdField,
                                                    timestampField,
                                                    itemId.longValue(),
                                                    OpType.INDEX);
                                        } else {
                                            paramMap.put(itemIdField,
                                                    itemId.longValue());
                                            handlePreferenceRequest(request,
                                                    channel, requestMap,
                                                    paramMap);
                                        }
                                        return;
                                    }
                                }
                                throw new OperationFailedException(
                                        "Item does not have " + itemIdField
                                                + ": " + searchHits[0]);
                            }
                        } catch (final Exception e) {
                            onFailure(e);
                        }
                    }

                    @Override
                    public void onFailure(final Throwable t) {
                        @SuppressWarnings("unchecked")
                        List<Throwable> errorList = (List<Throwable>) paramMap
                                .get(ERROR_LIST);
                        if (errorList == null) {
                            errorList = new ArrayList<>();
                            paramMap.put(ERROR_LIST, errorList);
                        }
                        if (errorList.size() >= maxRetryCount) {
                            sendErrorResponse(request, channel, t);
                        } else {
                            errorList.add(t);
                            handleItemIndexCreation(request, channel,
                                    requestMap, paramMap);
                        }
                    }
                });
    }

    private void handleItemIndexCreation(final RestRequest request,
            final RestChannel channel, final Map<String, Object> requestMap,
            final Map<String, Object> paramMap) {
        final String index = request
                .param("item_index", request.param("index"));

        client.admin().indices().prepareExists(index)
                .execute(new ActionListener<IndicesExistsResponse>() {

                    @Override
                    public void onResponse(
                            final IndicesExistsResponse indicesExistsResponse) {
                        if (indicesExistsResponse.isExists()) {
                            handleItemMappingCreation(request, channel,
                                    requestMap, paramMap);
                        } else {
                            client.admin()
                                    .indices()
                                    .prepareCreate(index)
                                    .execute(
                                            new ActionListener<CreateIndexResponse>() {

                                                @Override
                                                public void onResponse(
                                                        final CreateIndexResponse createIndexResponse) {
                                                    if (createIndexResponse
                                                            .isAcknowledged()) {
                                                        handleItemMappingCreation(
                                                                request,
                                                                channel,
                                                                requestMap,
                                                                paramMap);
                                                    } else {
                                                        onFailure(new OperationFailedException(
                                                                "Failed to create "
                                                                        + index));
                                                    }
                                                }

                                                @Override
                                                public void onFailure(
                                                        final Throwable t) {
                                                    sendErrorResponse(request,
                                                            channel, t);
                                                }
                                            });
                        }
                    }

                    @Override
                    public void onFailure(final Throwable t) {
                        sendErrorResponse(request, channel, t);
                    }
                });
    }

    private void handleItemMappingCreation(final RestRequest request,
            final RestChannel channel, final Map<String, Object> requestMap,
            final Map<String, Object> paramMap) {
        final String index = request
                .param("item_index", request.param("index"));
        final String type = request
                .param("item_type", TasteConstants.ITEM_TYPE);
        final String itemIdField = request.param(FIELD_ITEM_ID,
                TasteConstants.ITEM_ID_FIELD);
        final String timestampField = request.param(FIELD_TIMESTAMP,
                TasteConstants.TIMESTAMP_FIELD);

        try {
            final XContentBuilder builder = XContentFactory.jsonBuilder()//
                    .startObject()//
                    .startObject(type)//
                    .startObject("properties")//

                    // @timestamp
                    .startObject(timestampField)//
                    .field("type", "date")//
                    .field("format", "dateOptionalTime")//
                    .endObject()//

                    // item_id
                    .startObject(itemIdField)//
                    .field("type", "long")//
                    .endObject()//

                    // id
                    .startObject("id")//
                    .field("type", "string")//
                    .field("index", "not_analyzed")//
                    .endObject()//

                    .endObject()//
                    .endObject()//
                    .endObject();

            client.admin().indices().preparePutMapping(index).setType(type)
                    .setSource(builder)
                    .execute(new ActionListener<PutMappingResponse>() {

                        @Override
                        public void onResponse(
                                final PutMappingResponse queueMappingResponse) {
                            if (queueMappingResponse.isAcknowledged()) {
                                handleItemRequest(request, channel, requestMap,
                                        paramMap);
                            } else {
                                onFailure(new OperationFailedException(
                                        "Failed to create mapping for " + index
                                                + "/" + type));
                            }
                        }

                        @Override
                        public void onFailure(final Throwable t) {
                            sendErrorResponse(request, channel, t);
                        }
                    });
        } catch (final Exception e) {
            sendErrorResponse(request, channel, e);
        }
    }

    private void handleItemCreation(final RestRequest request,
            final RestChannel channel, final Map<String, Object> requestMap,
            final Map<String, Object> paramMap,
            final Map<String, Object> itemMap, final String index,
            final String type, final String itemIdField,
            final String timestampField) {
        client.prepareSearch(index).setTypes(type)
                .setQuery(QueryBuilders.matchAllQuery()).addField(itemIdField)
                .addSort(itemIdField, SortOrder.DESC).setSize(1)
                .execute(new ActionListener<SearchResponse>() {
                    @Override
                    public void onResponse(final SearchResponse response) {
                        try {
                            validateRespose(response);

                            Number currentId = null;
                            final SearchHits hits = response.getHits();
                            if (hits.getTotalHits() != 0) {
                                final SearchHit[] searchHits = hits.getHits();
                                final SearchHitField field = searchHits[0]
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
                            handleItemUpdate(request, channel, requestMap,
                                    paramMap, itemMap, index, type,
                                    itemIdField, timestampField, itemId,
                                    OpType.CREATE);
                        } catch (final Exception e) {
                            sendErrorResponse(request, channel, e);
                        }
                    }

                    @Override
                    public void onFailure(final Throwable t) {
                        sendErrorResponse(request, channel, t);
                    }
                });
    }

    private void handleItemUpdate(final RestRequest request,
            final RestChannel channel, final Map<String, Object> requestMap,
            final Map<String, Object> paramMap,
            final Map<String, Object> itemMap, final String index,
            final String type, final String itemIdField,
            final String timestampField, final Long itemId, final OpType opType) {
        itemMap.put(itemIdField, itemId);
        itemMap.put(timestampField, new Date());
        client.prepareIndex(index, type, itemId.toString()).setSource(itemMap)
                .setRefresh(true).setOpType(opType)
                .execute(new ActionListener<IndexResponse>() {

                    @Override
                    public void onResponse(final IndexResponse response) {
                        paramMap.put(itemIdField, itemId);
                        handlePreferenceRequest(request, channel, requestMap,
                                paramMap);
                    }

                    @Override
                    public void onFailure(final Throwable t) {
                        sendErrorResponse(request, channel, t);
                    }
                });
    }

    private void handlePreferenceRequest(final RestRequest request,
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
        } else if (timestampObj instanceof Number) {
            timestamp = new Date(((Number) timestampObj).longValue());
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
                            builder.field("acknowledged", true);
                            builder.endObject();
                            channel.sendResponse(new XContentRestResponse(
                                    request, OK, builder));
                        } catch (final IOException e) {
                            sendErrorResponse(request, channel, e);
                        }
                    }

                    @Override
                    public void onFailure(final Throwable t) {
                        @SuppressWarnings("unchecked")
                        List<Throwable> errorList = (List<Throwable>) paramMap
                                .get(ERROR_LIST);
                        if (errorList == null) {
                            errorList = new ArrayList<>();
                            paramMap.put(ERROR_LIST, errorList);
                        }
                        if (errorList.size() >= maxRetryCount) {
                            sendErrorResponse(request, channel, t);
                        } else {
                            errorList.add(t);
                            handlePreferenceIndexCreation(request, channel,
                                    requestMap, paramMap);
                        }
                    }
                });
    }

    private void handlePreferenceIndexCreation(final RestRequest request,
            final RestChannel channel, final Map<String, Object> requestMap,
            final Map<String, Object> paramMap) {
        final String index = request.param("index");

        client.admin().indices().prepareExists(index)
                .execute(new ActionListener<IndicesExistsResponse>() {

                    @Override
                    public void onResponse(
                            final IndicesExistsResponse indicesExistsResponse) {
                        if (indicesExistsResponse.isExists()) {
                            handlePreferenceMappingCreation(request, channel,
                                    requestMap, paramMap);
                        } else {
                            client.admin()
                                    .indices()
                                    .prepareCreate(index)
                                    .execute(
                                            new ActionListener<CreateIndexResponse>() {

                                                @Override
                                                public void onResponse(
                                                        final CreateIndexResponse createIndexResponse) {
                                                    if (createIndexResponse
                                                            .isAcknowledged()) {
                                                        handlePreferenceMappingCreation(
                                                                request,
                                                                channel,
                                                                requestMap,
                                                                paramMap);
                                                    } else {
                                                        onFailure(new OperationFailedException(
                                                                "Failed to create "
                                                                        + index));
                                                    }
                                                }

                                                @Override
                                                public void onFailure(
                                                        final Throwable t) {
                                                    sendErrorResponse(request,
                                                            channel, t);
                                                }
                                            });
                        }
                    }

                    @Override
                    public void onFailure(final Throwable t) {
                        sendErrorResponse(request, channel, t);
                    }
                });
    }

    private void handlePreferenceMappingCreation(final RestRequest request,
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

        try {
            final XContentBuilder builder = XContentFactory.jsonBuilder()//
                    .startObject()//
                    .startObject(type)//
                    .startObject("properties")//

                    // @timestamp
                    .startObject(timestampField)//
                    .field("type", "date")//
                    .field("format", "dateOptionalTime")//
                    .endObject()//

                    // user_id
                    .startObject(userIdField)//
                    .field("type", "long")//
                    .endObject()//

                    // item_id
                    .startObject(itemIdField)//
                    .field("type", "long")//
                    .endObject()//

                    // value
                    .startObject(valueField)//
                    .field("type", "double")//
                    .endObject()//

                    .endObject()//
                    .endObject()//
                    .endObject();

            client.admin().indices().preparePutMapping(index).setType(type)
                    .setSource(builder)
                    .execute(new ActionListener<PutMappingResponse>() {

                        @Override
                        public void onResponse(
                                final PutMappingResponse queueMappingResponse) {
                            if (queueMappingResponse.isAcknowledged()) {
                                handlePreferenceRequest(request, channel,
                                        requestMap, paramMap);
                            } else {
                                onFailure(new OperationFailedException(
                                        "Failed to create mapping for " + index
                                                + "/" + type));
                            }
                        }

                        @Override
                        public void onFailure(final Throwable t) {
                            sendErrorResponse(request, channel, t);
                        }
                    });
        } catch (final Exception e) {
            sendErrorResponse(request, channel, e);
        }
    }

    private void validateRespose(final SearchResponse response) {
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
