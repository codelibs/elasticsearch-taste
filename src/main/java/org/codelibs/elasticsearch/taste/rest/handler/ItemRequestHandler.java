package org.codelibs.elasticsearch.taste.rest.handler;

import java.security.InvalidParameterException;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;
import java.util.Map;

import org.codelibs.elasticsearch.taste.TasteConstants;
import org.codelibs.elasticsearch.taste.exception.OperationFailedException;
import org.elasticsearch.action.ActionListener;
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
        final String index = params.param("item_index", params.param("index"));
        final String itemType = params.param("item_type",
                TasteConstants.ITEM_TYPE);
        final String itemIdField = params.param(FIELD_ITEM_ID,
                TasteConstants.ITEM_ID_FIELD);
        final String timestampField = params.param(FIELD_TIMESTAMP,
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

        try {
            client.prepareSearch(index).setTypes(itemType)
                    .setQuery(QueryBuilders.termQuery("id", id))
                    .addField(itemIdField)
                    .addSort(timestampField, SortOrder.DESC).setSize(1)
                    .execute(new ActionListener<SearchResponse>() {

                        @Override
                        public void onResponse(final SearchResponse response) {
                            try {
                                validateRespose(response);
                                final String updateType = params
                                        .param("update");

                                final SearchHits hits = response.getHits();
                                if (hits.getTotalHits() == 0) {
                                    doItemCreation(params, listener,
                                            requestMap, paramMap, itemMap,
                                            index, itemType, itemIdField,
                                            timestampField, chain);
                                } else {
                                    final SearchHit[] searchHits = hits
                                            .getHits();
                                    final SearchHitField field = searchHits[0]
                                            .getFields().get(itemIdField);
                                    if (field != null) {
                                        final Number itemId = field.getValue();
                                        if (itemId != null) {
                                            if (TasteConstants.TRUE
                                                    .equalsIgnoreCase(updateType)
                                                    || TasteConstants.YES
                                                            .equalsIgnoreCase(updateType)) {
                                                doItemUpdate(params, listener,
                                                        requestMap, paramMap,
                                                        itemMap, index,
                                                        itemType, itemIdField,
                                                        timestampField,
                                                        itemId.longValue(),
                                                        OpType.INDEX, chain);
                                            } else {
                                                paramMap.put(itemIdField,
                                                        itemId.longValue());
                                                chain.execute(params, listener,
                                                        requestMap, paramMap);
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
                                listener.onError(t);
                            } else {
                                errorList.add(t);
                                doItemIndexCreation(params, listener,
                                        requestMap, paramMap, chain);
                            }
                        }
                    });
        } catch (final Exception e) {
            if (e instanceof EsRejectedExecutionException) {
                sleep();
            }
            @SuppressWarnings("unchecked")
            List<Throwable> errorList = (List<Throwable>) paramMap
                    .get(ERROR_LIST);
            if (errorList == null) {
                errorList = new ArrayList<>();
                paramMap.put(ERROR_LIST, errorList);
            }
            if (errorList.size() >= maxRetryCount) {
                listener.onError(e);
            } else {
                errorList.add(e);
                doItemIndexCreation(params, listener, requestMap, paramMap,
                        chain);
            }
        }
    }

    private void doItemIndexCreation(final Params params,
            final RequestHandler.OnErrorListener listener,
            final Map<String, Object> requestMap,
            final Map<String, Object> paramMap, final RequestHandlerChain chain) {
        final String index = params.param("item_index", params.param("index"));

        client.admin().indices().prepareExists(index)
                .execute(new ActionListener<IndicesExistsResponse>() {

                    @Override
                    public void onResponse(
                            final IndicesExistsResponse indicesExistsResponse) {
                        if (indicesExistsResponse.isExists()) {
                            doItemMappingCreation(params, listener, requestMap,
                                    paramMap, chain);
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
                                                        doItemMappingCreation(
                                                                params,
                                                                listener,
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
                                                    if (t instanceof IndexAlreadyExistsException) {
                                                        doItemIndexCreation(
                                                                params,
                                                                listener,
                                                                requestMap,
                                                                paramMap, chain);
                                                    } else {
                                                        listener.onError(t);
                                                    }
                                                }
                                            });
                        }
                    }

                    @Override
                    public void onFailure(final Throwable t) {
                        listener.onError(t);
                    }
                });
    }

    private void doItemMappingCreation(final Params params,
            final RequestHandler.OnErrorListener listener,
            final Map<String, Object> requestMap,
            final Map<String, Object> paramMap, final RequestHandlerChain chain) {
        final String index = params.param("item_index", params.param("index"));
        final String type = params.param("item_type", TasteConstants.ITEM_TYPE);
        final String itemIdField = params.param(FIELD_ITEM_ID,
                TasteConstants.ITEM_ID_FIELD);
        final String timestampField = params.param(FIELD_TIMESTAMP,
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
                                execute(params, listener, requestMap, paramMap,
                                        chain);
                            } else {
                                onFailure(new OperationFailedException(
                                        "Failed to create mapping for " + index
                                                + "/" + type));
                            }
                        }

                        @Override
                        public void onFailure(final Throwable t) {
                            listener.onError(t);
                        }
                    });
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
                            doItemUpdate(params, listener, requestMap,
                                    paramMap, itemMap, index, type,
                                    itemIdField, timestampField, itemId,
                                    OpType.CREATE, chain);
                        } catch (final Exception e) {
                            listener.onError(e);
                        }
                    }

                    @Override
                    public void onFailure(final Throwable t) {
                        listener.onError(t);
                    }
                });
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
        client.prepareIndex(index, type, itemId.toString()).setSource(itemMap)
                .setRefresh(true).setOpType(opType)
                .execute(new ActionListener<IndexResponse>() {

                    @Override
                    public void onResponse(final IndexResponse response) {
                        paramMap.put(itemIdField, itemId);
                        chain.execute(params, listener, requestMap, paramMap);
                    }

                    @Override
                    public void onFailure(final Throwable t) {
                        if (t instanceof DocumentAlreadyExistsException) {
                            execute(params, listener, requestMap, paramMap,
                                    chain);
                        } else {
                            listener.onError(t);
                        }
                    }
                });
    }

}
