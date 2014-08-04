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

public class ItemRequestHandler extends DefaultRequestHandler {
    public ItemRequestHandler(final Settings settings, final Client client) {
        super(settings, client);
    }

    public boolean hasItem(final Map<String, Object> requestMap) {
        return requestMap.containsKey("item");
    }

    @Override
    public void execute(final Params params,
            final RequestHandler.OnErrorListener listener,
            final Map<String, Object> requestMap,
            final Map<String, Object> paramMap, final RequestHandlerChain chain) {
        final String index = params.param(
                TasteConstants.REQUEST_PARAM_ITEM_INDEX, params.param("index"));
        final String itemType = params.param(
                TasteConstants.REQUEST_PARAM_ITEM_TYPE,
                TasteConstants.ITEM_TYPE);
        final String itemIdField = params.param(
                TasteConstants.REQUEST_PARAM_ITEM_ID_FIELD,
                TasteConstants.ITEM_ID_FIELD);
        final String idField = params.param(
                TasteConstants.REQUEST_PARAM_ID_FIELD, "id");
        final String timestampField = params.param(
                TasteConstants.REQUEST_PARAM_TIMESTAMP_FIELD,
                TasteConstants.TIMESTAMP_FIELD);

        @SuppressWarnings("unchecked")
        final Map<String, Object> itemMap = (Map<String, Object>) requestMap
                .get("item");
        if (itemMap == null) {
            throw new InvalidParameterException("Item is null.");
        }
        Object systemId = itemMap.get("system_id");
        if (systemId == null) {
            systemId = itemMap.remove(idField);
            if (systemId == null) {
                throw new InvalidParameterException("Item ID is null.");
            }
            itemMap.put("system_id", systemId);
        }

        try {
            final OnResponseListener<SearchResponse> responseListener = response -> {
                validateRespose(response);
                final String updateType = params.param("update");

                final SearchHits hits = response.getHits();
                if (hits.getTotalHits() == 0) {
                    doItemCreation(params, listener, requestMap, paramMap,
                            itemMap, index, itemType, itemIdField,
                            timestampField, chain);
                } else {
                    final SearchHit[] searchHits = hits.getHits();
                    final SearchHitField field = searchHits[0].getFields().get(
                            itemIdField);
                    if (field != null) {
                        final Number itemId = field.getValue();
                        if (itemId != null) {
                            if (TasteConstants.TRUE
                                    .equalsIgnoreCase(updateType)
                                    || TasteConstants.YES
                                            .equalsIgnoreCase(updateType)) {
                                doItemUpdate(params, listener, requestMap,
                                        paramMap, itemMap, index, itemType,
                                        itemIdField, timestampField,
                                        itemId.longValue(), OpType.INDEX, chain);
                            } else {
                                paramMap.put(itemIdField, itemId.longValue());
                                chain.execute(params, listener, requestMap,
                                        paramMap);
                            }
                            return;
                        }
                    }
                    throw new OperationFailedException("Item does not have "
                            + itemIdField + ": " + searchHits[0]);
                }
            };
            final OnFailureListener failureListener = t -> {
                final List<Throwable> errorList = getErrorList(paramMap);
                if (errorList.size() >= maxRetryCount) {
                    listener.onError(t);
                } else {
                    sleep(t);
                    errorList.add(t);
                    doItemIndexExists(params, listener, requestMap, paramMap,
                            chain);
                }
            };
            client.prepareSearch(index).setTypes(itemType)
                    .setQuery(QueryBuilders.termQuery("system_id", systemId))
                    .addField(itemIdField)
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

    private void doItemIndexExists(final Params params,
            final RequestHandler.OnErrorListener listener,
            final Map<String, Object> requestMap,
            final Map<String, Object> paramMap, final RequestHandlerChain chain) {
        final String index = params.param(
                TasteConstants.REQUEST_PARAM_ITEM_INDEX, params.param("index"));

        try {
            indexCreationLock.lock();
            final IndicesExistsResponse indicesExistsResponse = client.admin()
                    .indices().prepareExists(index).execute().actionGet();
            if (indicesExistsResponse.isExists()) {
                doItemMappingCreation(params, listener, requestMap, paramMap,
                        chain);
            } else {
                doItemIndexCreation(params, listener, requestMap, paramMap,
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

    private void doItemIndexCreation(final Params params,
            final RequestHandler.OnErrorListener listener,
            final Map<String, Object> requestMap,
            final Map<String, Object> paramMap,
            final RequestHandlerChain chain, final String index) {
        try {
            final CreateIndexResponse createIndexResponse = client.admin()
                    .indices().prepareCreate(index).execute().actionGet();
            if (createIndexResponse.isAcknowledged()) {
                doItemMappingCreation(params, listener, requestMap, paramMap,
                        chain);
            } else {
                listener.onError(new OperationFailedException(
                        "Failed to create " + index));
            }
        } catch (final IndexAlreadyExistsException e) {
            fork(() -> doItemIndexExists(params, listener, requestMap,
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

    private void doItemMappingCreation(final Params params,
            final RequestHandler.OnErrorListener listener,
            final Map<String, Object> requestMap,
            final Map<String, Object> paramMap, final RequestHandlerChain chain) {
        final String index = params.param(
                TasteConstants.REQUEST_PARAM_ITEM_INDEX, params.param("index"));
        final String type = params.param(
                TasteConstants.REQUEST_PARAM_ITEM_TYPE,
                TasteConstants.ITEM_TYPE);
        final String itemIdField = params.param(
                TasteConstants.REQUEST_PARAM_ITEM_ID_FIELD,
                TasteConstants.ITEM_ID_FIELD);
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

                    // item_id
                    .startObject(itemIdField)//
                    .field("type", "long")//
                    .endObject()//

                    // system_id
                    .startObject("system_id")//
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

    private void doItemCreation(final Params params,
            final RequestHandler.OnErrorListener listener,
            final Map<String, Object> requestMap,
            final Map<String, Object> paramMap,
            final Map<String, Object> itemMap, final String index,
            final String type, final String itemIdField,
            final String timestampField, final RequestHandlerChain chain) {
        final OnResponseListener<SearchResponse> responseListener = response -> {
            validateRespose(response);

            Number currentId = null;
            final SearchHits hits = response.getHits();
            if (hits.getTotalHits() != 0) {
                final SearchHit[] searchHits = hits.getHits();
                final SearchHitField field = searchHits[0].getFields().get(
                        itemIdField);
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
            doItemUpdate(params, listener, requestMap, paramMap, itemMap,
                    index, type, itemIdField, timestampField, itemId,
                    OpType.CREATE, chain);
        };
        final OnFailureListener failureListener = t -> {
            final List<Throwable> errorList = getErrorList(paramMap);
            if (errorList.size() >= maxRetryCount) {
                listener.onError(t);
            } else {
                sleep(t);
                errorList.add(t);
                execute(params, listener, requestMap, paramMap, chain);
            }
        };
        client.prepareSearch(index).setTypes(type)
                .setQuery(QueryBuilders.matchAllQuery()).addField(itemIdField)
                .addSort(itemIdField, SortOrder.DESC).setSize(1)
                .execute(on(responseListener, failureListener));
    }

    private void doItemUpdate(final Params params,
            final RequestHandler.OnErrorListener listener,
            final Map<String, Object> requestMap,
            final Map<String, Object> paramMap,
            final Map<String, Object> itemMap, final String index,
            final String type, final String itemIdField,
            final String timestampField, final Long itemId,
            final OpType opType, final RequestHandlerChain chain) {
        itemMap.put(itemIdField, itemId);
        itemMap.put(timestampField, new Date());
        final OnResponseListener<IndexResponse> responseListener = response -> {
            paramMap.put(itemIdField, itemId);
            chain.execute(params, listener, requestMap, paramMap);
        };
        final OnFailureListener failureListener = t -> {
            sleep(t);
            if (t instanceof DocumentAlreadyExistsException
                    || t instanceof EsRejectedExecutionException) {
                execute(params, listener, requestMap, paramMap, chain);
            } else {
                listener.onError(t);
            }
        };
        client.prepareIndex(index, type, itemId.toString()).setSource(itemMap)
                .setRefresh(true).setOpType(opType)
                .execute(on(responseListener, failureListener));
    }

}
