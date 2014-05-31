package org.codelibs.elasticsearch.taste.rest.handler;

import java.security.InvalidParameterException;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;
import java.util.Map;

import org.codelibs.elasticsearch.taste.TasteConstants;
import org.codelibs.elasticsearch.taste.rest.exception.OperationFailedException;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.admin.indices.create.CreateIndexResponse;
import org.elasticsearch.action.admin.indices.exists.indices.IndicesExistsResponse;
import org.elasticsearch.action.admin.indices.mapping.put.PutMappingResponse;
import org.elasticsearch.action.index.IndexRequest.OpType;
import org.elasticsearch.action.index.IndexResponse;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.client.Client;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentFactory;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.rest.RestChannel;
import org.elasticsearch.rest.RestRequest;
import org.elasticsearch.search.SearchHit;
import org.elasticsearch.search.SearchHitField;
import org.elasticsearch.search.SearchHits;
import org.elasticsearch.search.sort.SortOrder;

public class UserRequestHandler extends RequestHandler {

    public UserRequestHandler(final Settings settings, final Client client) {
        super(settings, client);
    }

    public boolean hasUser(final Map<String, Object> requestMap) {
        return requestMap.containsKey("user");
    }

    @Override
    public void process(final RestRequest request, final RestChannel channel,
            final Map<String, Object> requestMap,
            final Map<String, Object> paramMap, final Chain chain) {
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

        try {
            client.prepareSearch(index).setTypes(userType)
                    .setQuery(QueryBuilders.termQuery("id", id))
                    .addField(userIdField)
                    .addSort(timestampField, SortOrder.DESC).setSize(1)
                    .execute(new ActionListener<SearchResponse>() {

                        @Override
                        public void onResponse(final SearchResponse response) {
                            try {
                                validateRespose(response);
                                final String updateType = request
                                        .param("update");

                                final SearchHits hits = response.getHits();
                                if (hits.getTotalHits() == 0) {
                                    handleUserCreation(request, channel,
                                            requestMap, paramMap, userMap,
                                            index, userType, userIdField,
                                            timestampField, chain);
                                } else {
                                    final SearchHit[] searchHits = hits
                                            .getHits();
                                    final SearchHitField field = searchHits[0]
                                            .getFields().get(userIdField);
                                    if (field != null) {
                                        final Number userId = field.getValue();
                                        if (userId != null) {
                                            if (TasteConstants.TRUE
                                                    .equalsIgnoreCase(updateType)
                                                    || TasteConstants.YES
                                                            .equalsIgnoreCase(updateType)) {
                                                handleUserUpdate(request,
                                                        channel, requestMap,
                                                        paramMap, userMap,
                                                        index, userType,
                                                        userIdField,
                                                        timestampField,
                                                        userId.longValue(),
                                                        OpType.INDEX, chain);

                                            } else {
                                                paramMap.put(userIdField,
                                                        userId.longValue());
                                                chain.process(request, channel,
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
                                        requestMap, paramMap, chain);
                            }
                        }
                    });
        } catch (final Exception e) {
            @SuppressWarnings("unchecked")
            List<Throwable> errorList = (List<Throwable>) paramMap
                    .get(ERROR_LIST);
            if (errorList == null) {
                errorList = new ArrayList<>();
                paramMap.put(ERROR_LIST, errorList);
            }
            if (errorList.size() >= maxRetryCount) {
                sendErrorResponse(request, channel, e);
            } else {
                errorList.add(e);
                handleUserIndexCreation(request, channel, requestMap, paramMap,
                        chain);
            }
        }
    }

    private void handleUserIndexCreation(final RestRequest request,
            final RestChannel channel, final Map<String, Object> requestMap,
            final Map<String, Object> paramMap, final Chain chain) {
        final String index = request
                .param("user_index", request.param("index"));

        client.admin().indices().prepareExists(index)
                .execute(new ActionListener<IndicesExistsResponse>() {

                    @Override
                    public void onResponse(
                            final IndicesExistsResponse indicesExistsResponse) {
                        if (indicesExistsResponse.isExists()) {
                            handleUserMappingCreation(request, channel,
                                    requestMap, paramMap, chain);
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
                                                                paramMap, chain);
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
            final Map<String, Object> paramMap, final Chain chain) {
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
                                process(request, channel, requestMap, paramMap,
                                        chain);
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
            final String timestampField, final Chain chain) {
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
                                    OpType.CREATE, chain);
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
            final String timestampField, final Long userId,
            final OpType opType, final Chain chain) {
        userMap.put(userIdField, userId);
        userMap.put(timestampField, new Date());
        client.prepareIndex(index, type, userId.toString()).setSource(userMap)
                .setRefresh(true).setOpType(opType)
                .execute(new ActionListener<IndexResponse>() {

                    @Override
                    public void onResponse(final IndexResponse response) {
                        paramMap.put(userIdField, userId);
                        chain.process(request, channel, requestMap, paramMap);
                    }

                    @Override
                    public void onFailure(final Throwable t) {
                        sendErrorResponse(request, channel, t);
                    }
                });
    }

}
