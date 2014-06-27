package org.codelibs.elasticsearch.taste.rest.handler;

import static org.codelibs.elasticsearch.util.action.ListenerUtils.on;

import java.security.InvalidParameterException;
import java.util.Date;
import java.util.List;
import java.util.Map;

import org.codelibs.elasticsearch.taste.TasteConstants;
import org.codelibs.elasticsearch.taste.exception.OperationFailedException;
import org.codelibs.elasticsearch.util.action.ListenerUtils.OnFailureListener;
import org.codelibs.elasticsearch.util.action.ListenerUtils.OnResponseListener;
import org.elasticsearch.action.admin.cluster.health.ClusterHealthResponse;
import org.elasticsearch.action.admin.indices.create.CreateIndexResponse;
import org.elasticsearch.action.admin.indices.exists.indices.IndicesExistsResponse;
import org.elasticsearch.action.admin.indices.mapping.put.PutMappingResponse;
import org.elasticsearch.action.index.IndexRequest.OpType;
import org.elasticsearch.action.index.IndexResponse;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.client.Client;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.util.concurrent.EsRejectedExecutionException;
import org.elasticsearch.common.xcontent.ToXContent.Params;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentFactory;
import org.elasticsearch.index.engine.DocumentAlreadyExistsException;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.indices.IndexAlreadyExistsException;
import org.elasticsearch.search.SearchHit;
import org.elasticsearch.search.SearchHitField;
import org.elasticsearch.search.SearchHits;
import org.elasticsearch.search.sort.SortOrder;

public class UserRequestHandler extends DefaultRequestHandler {

    public UserRequestHandler(final Settings settings, final Client client) {
        super(settings, client);
    }

    public boolean hasUser(final Map<String, Object> requestMap) {
        return requestMap.containsKey("user");
    }

    @Override
    public void execute(final Params params,
            final RequestHandler.OnErrorListener listener,
            final Map<String, Object> requestMap,
            final Map<String, Object> paramMap, final RequestHandlerChain chain) {
        final String index = params.param(
                TasteConstants.REQUEST_PARAM_USER_INDEX, params.param("index"));
        final String userType = params.param(
                TasteConstants.REQUEST_PARAM_USER_TYPE,
                TasteConstants.USER_TYPE);
        final String userIdField = params.param(
                TasteConstants.REQUEST_PARAM_USER_ID_FIELD,
                TasteConstants.USER_ID_FIELD);
        final String timestampField = params.param(
                TasteConstants.REQUEST_PARAM_TIMESTAMP_FIELD,
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
            final OnResponseListener<SearchResponse> responseListener = response -> {
                validateRespose(response);
                final String updateType = params.param("update");

                final SearchHits hits = response.getHits();
                if (hits.getTotalHits() == 0) {
                    doUserCreation(params, listener, requestMap, paramMap,
                            userMap, index, userType, userIdField,
                            timestampField, chain);
                } else {
                    final SearchHit[] searchHits = hits.getHits();
                    final SearchHitField field = searchHits[0].getFields().get(
                            userIdField);
                    if (field != null) {
                        final Number userId = field.getValue();
                        if (userId != null) {
                            if (TasteConstants.TRUE
                                    .equalsIgnoreCase(updateType)
                                    || TasteConstants.YES
                                            .equalsIgnoreCase(updateType)) {
                                doUserUpdate(params, listener, requestMap,
                                        paramMap, userMap, index, userType,
                                        userIdField, timestampField,
                                        userId.longValue(), OpType.INDEX, chain);

                            } else {
                                paramMap.put(userIdField, userId.longValue());
                                chain.execute(params, listener, requestMap,
                                        paramMap);
                            }
                            return;
                        }
                    }
                    throw new OperationFailedException("User does not have "
                            + userIdField + ": " + searchHits[0]);
                }
            };
            final OnFailureListener failureListener = t -> {
                final List<Throwable> errorList = getErrorList(paramMap);
                if (errorList.size() >= maxRetryCount) {
                    listener.onError(t);
                } else {
                    sleep(t);
                    errorList.add(t);
                    doUserIndexExists(params, listener, requestMap, paramMap,
                            chain);
                }
            };
            client.prepareSearch(index).setTypes(userType)
                    .setQuery(QueryBuilders.termQuery("id", id))
                    .addField(userIdField)
                    .addSort(timestampField, SortOrder.DESC).setSize(1)
                    .execute(on(responseListener, failureListener));

        } catch (final Exception e) {
            final List<Throwable> errorList = getErrorList(paramMap);
            if (errorList.size() >= maxRetryCount) {
                listener.onError(e);
            } else {
                sleep(e);
                errorList.add(e);
                fork(() -> execute(params, listener, requestMap, paramMap,
                        chain));
            }
        }
    }

    private void doUserIndexExists(final Params params,
            final RequestHandler.OnErrorListener listener,
            final Map<String, Object> requestMap,
            final Map<String, Object> paramMap, final RequestHandlerChain chain) {
        final String index = params.param(
                TasteConstants.REQUEST_PARAM_USER_INDEX, params.param("index"));

        try {
            indexCreationLock.lock();
            final IndicesExistsResponse indicesExistsResponse = client.admin()
                    .indices().prepareExists(index).execute().actionGet();
            if (indicesExistsResponse.isExists()) {
                doUserMappingCreation(params, listener, requestMap, paramMap,
                        chain);
            } else {
                doUserIndexCreation(params, listener, requestMap, paramMap,
                        chain, index);
            }
        } catch (final Exception e) {
            final List<Throwable> errorList = getErrorList(paramMap);
            if (errorList.size() >= maxRetryCount) {
                listener.onError(e);
            } else {
                sleep(e);
                errorList.add(e);
                fork(() -> execute(params, listener, requestMap, paramMap,
                        chain));
            }
        } finally {
            indexCreationLock.unlock();
        }
    }

    private void doUserIndexCreation(final Params params,
            final RequestHandler.OnErrorListener listener,
            final Map<String, Object> requestMap,
            final Map<String, Object> paramMap,
            final RequestHandlerChain chain, final String index) {
        try {
            final CreateIndexResponse createIndexResponse = client.admin()
                    .indices().prepareCreate(index).execute().actionGet();
            if (createIndexResponse.isAcknowledged()) {
                doUserMappingCreation(params, listener, requestMap, paramMap,
                        chain);
            } else {
                listener.onError(new OperationFailedException(
                        "Failed to create " + index));
            }
        } catch (final IndexAlreadyExistsException e) {
            fork(() -> doUserIndexExists(params, listener, requestMap,
                    paramMap, chain));
        } catch (final Exception e) {
            final List<Throwable> errorList = getErrorList(paramMap);
            if (errorList.size() >= maxRetryCount) {
                listener.onError(e);
            } else {
                sleep(e);
                errorList.add(e);
                fork(() -> execute(params, listener, requestMap, paramMap,
                        chain));
            }
        }
    }

    private void doUserMappingCreation(final Params params,
            final RequestHandler.OnErrorListener listener,
            final Map<String, Object> requestMap,
            final Map<String, Object> paramMap, final RequestHandlerChain chain) {
        final String index = params.param(
                TasteConstants.REQUEST_PARAM_USER_INDEX, params.param("index"));
        final String type = params.param(
                TasteConstants.REQUEST_PARAM_USER_TYPE,
                TasteConstants.USER_TYPE);
        final String userIdField = params.param(
                TasteConstants.REQUEST_PARAM_USER_ID_FIELD,
                TasteConstants.USER_ID_FIELD);
        final String timestampField = params.param(
                TasteConstants.REQUEST_PARAM_TIMESTAMP_FIELD,
                TasteConstants.TIMESTAMP_FIELD);

        try {
            final ClusterHealthResponse healthResponse = client
                    .admin()
                    .cluster()
                    .prepareHealth(index)
                    .setWaitForYellowStatus()
                    .setTimeout(
                            params.param("timeout",
                                    DEFAULT_HEALTH_REQUEST_TIMEOUT)).execute()
                    .actionGet();
            if (healthResponse.isTimedOut()) {
                listener.onError(new OperationFailedException(
                        "Failed to create index: " + index + "/" + type));
            }

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

            final PutMappingResponse mappingResponse = client.admin().indices()
                    .preparePutMapping(index).setType(type).setSource(builder)
                    .execute().actionGet();
            if (mappingResponse.isAcknowledged()) {
                fork(() -> execute(params, listener, requestMap, paramMap,
                        chain));
            } else {
                listener.onError(new OperationFailedException(
                        "Failed to create mapping for " + index + "/" + type));
            }
        } catch (final Exception e) {
            listener.onError(e);
        }
    }

    private void doUserCreation(final Params params,
            final RequestHandler.OnErrorListener listener,
            final Map<String, Object> requestMap,
            final Map<String, Object> paramMap,
            final Map<String, Object> userMap, final String index,
            final String type, final String userIdField,
            final String timestampField, final RequestHandlerChain chain) {
        final OnResponseListener<SearchResponse> responseListener = response -> {
            validateRespose(response);

            Number currentId = null;
            final SearchHits hits = response.getHits();
            if (hits.getTotalHits() != 0) {
                final SearchHit[] searchHits = hits.getHits();
                final SearchHitField field = searchHits[0].getFields().get(
                        userIdField);
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
            doUserUpdate(params, listener, requestMap, paramMap, userMap,
                    index, type, userIdField, timestampField, userId,
                    OpType.CREATE, chain);
        };
        final OnFailureListener failureListener = t -> {
            final List<Throwable> errorList = getErrorList(paramMap);
            if (errorList.size() >= maxRetryCount) {
                listener.onError(t);
            } else {
                sleep(t);
                execute(params, listener, requestMap, paramMap, chain);
            }
        };
        client.prepareSearch(index).setTypes(type)
                .setQuery(QueryBuilders.matchAllQuery()).addField(userIdField)
                .addSort(userIdField, SortOrder.DESC).setSize(1)
                .execute(on(responseListener, failureListener));
    }

    private void doUserUpdate(final Params params,
            final RequestHandler.OnErrorListener listener,
            final Map<String, Object> requestMap,
            final Map<String, Object> paramMap,
            final Map<String, Object> userMap, final String index,
            final String type, final String userIdField,
            final String timestampField, final Long userId,
            final OpType opType, final RequestHandlerChain chain) {
        userMap.put(userIdField, userId);
        userMap.put(timestampField, new Date());

        final OnResponseListener<IndexResponse> responseListener = response -> {
            paramMap.put(userIdField, userId);
            chain.execute(params, listener, requestMap, paramMap);
        };
        final OnFailureListener failureListener = t -> {
            if (t instanceof DocumentAlreadyExistsException
                    || t instanceof EsRejectedExecutionException) {
                sleep(t);
                execute(params, listener, requestMap, paramMap, chain);
            } else {
                listener.onError(t);
            }
        };
        client.prepareIndex(index, type, userId.toString()).setSource(userMap)
                .setRefresh(true).setOpType(opType)
                .execute(on(responseListener, failureListener));
    }

}
