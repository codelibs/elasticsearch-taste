package org.codelibs.elasticsearch.taste.rest;

import static org.codelibs.elasticsearch.util.action.ListenerUtils.on;
import static org.elasticsearch.common.xcontent.XContentFactory.jsonBuilder;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;

import org.codelibs.elasticsearch.taste.TasteConstants;
import org.codelibs.elasticsearch.taste.exception.NotFoundException;
import org.codelibs.elasticsearch.taste.exception.OperationFailedException;
import org.codelibs.elasticsearch.taste.exception.TasteException;
import org.codelibs.elasticsearch.util.action.ListenerUtils.OnResponseListener;
import org.codelibs.elasticsearch.util.lang.StringUtils;
import org.elasticsearch.action.get.GetResponse;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.client.Client;
import org.elasticsearch.common.cache.Cache;
import org.elasticsearch.common.cache.CacheBuilder;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.rest.BaseRestHandler;
import org.elasticsearch.rest.BytesRestResponse;
import org.elasticsearch.rest.RestChannel;
import org.elasticsearch.rest.RestController;
import org.elasticsearch.rest.RestRequest;
import org.elasticsearch.rest.RestStatus;
import org.elasticsearch.search.SearchHit;
import org.elasticsearch.search.SearchHitField;
import org.elasticsearch.search.SearchHits;
import org.elasticsearch.search.sort.SortOrder;

public class TasteSearchRestAction extends BaseRestHandler {

    private Cache<String, Map<String, Object>> cache;

    @Inject
    public TasteSearchRestAction(final Settings settings, final Client client,
            final RestController restController) {
        super(settings, client);

        final String size = settings.get("taste.cache.search.size", "1000");
        final String duration = settings.get("taste.cache.search.duration",
                "600000"); // 10min
        cache = CacheBuilder
                .newBuilder()
                .expireAfterAccess(Long.parseLong(duration),
                        TimeUnit.MILLISECONDS)
                        .maximumSize(Long.parseLong(size)).build();

        restController.registerHandler(RestRequest.Method.GET,
                "/{index}/_taste/{objectType}/{id}", this);
        restController.registerHandler(RestRequest.Method.GET,
                "/{index}/{type}/_taste/{objectType}/{id}", this);

    }

    @Override
    public void handleRequest(final RestRequest request,
            final RestChannel channel) {

        final Info info = new Info(request);

        final String id = request.param("id");
        if (StringUtils.isBlank(id)) {
            onError(channel, new NotFoundException("No id."));
            return;
        }

        if (info.getIdIndex() == null) {
            onError(channel, new NotFoundException("No search type."));
            return;
        }

        final OnResponseListener<SearchResponse> responseListener = searchResponse -> {
            final SearchHits hits = searchResponse.getHits();
            if (hits.totalHits() == 0) {
                onError(channel,
                        new NotFoundException("No " + info.getIdField()
                                + " data for " + id + " in "
                                + info.getIdIndex() + "/" + info.getIdType()));
                return;
            }
            final SearchHit hit = hits.getHits()[0];
            final SearchHitField field = hit.field(info.getIdField());
            final Number targetId = field.getValue();
            if (targetId == null) {
                onError(channel,
                        new NotFoundException("No " + info.getIdField()
                                + " for " + id + " in " + info.getIdIndex()
                                + "/" + info.getIdType()));
                return;
            }

            doSearchRequest(request, channel, info, targetId.longValue());
        };
        client.prepareSearch(info.getIdIndex()).setTypes(info.getIdType())
        .setQuery(QueryBuilders.termQuery("id", id))
        .addField(info.getIdField())
        .addSort(info.getTimestampField(), SortOrder.DESC)
        .execute(on(responseListener, t -> onError(channel, t)));

    }

    private void doSearchRequest(final RestRequest request,
            final RestChannel channel, final Info info, final long targetId) {

        final OnResponseListener<SearchResponse> responseListener = response -> {
            final SearchHits hits = response.getHits();
            if (hits.totalHits() == 0) {
                onError(channel,
                        new NotFoundException("No ID for " + targetId + " in "
                                + info.getTargetIndex() + "/"
                                + info.getTargetType()));
                return;
            }

            try {
                final XContentBuilder builder = jsonBuilder().startObject()//
                        .field("took", response.getTookInMillis())//
                        .field("timed_out", response.isTimedOut())//
                        .startObject("_shards")//
                        .field("total", response.getTotalShards())//
                        .field("successful", response.getSuccessfulShards())//
                        .field("failed", response.getFailedShards())//
                        .endObject()//
                        .startObject("hits")//
                        .field("total", hits.getTotalHits())//
                        .field("max_score", hits.getMaxScore())//
                        .startArray("hits");

                for (final SearchHit hit : hits.getHits()) {
                    final Map<String, Object> source = expandObjects(
                            hit.getSource(), info);
                    builder.startObject()//
                    .field("_index", hit.getIndex())//
                    .field("_type", hit.getType())//
                    .field("_id", hit.getId())//
                    .field("_score", hit.getScore())//
                    .field("_source", source)//
                    .endObject();//
                }

                builder.endArray()//
                .endObject()//
                .endObject();

                channel.sendResponse(new BytesRestResponse(RestStatus.OK,
                        builder));
            } catch (final IOException e) {
                throw new OperationFailedException(
                        "Failed to build a response.", e);
            }
        };
        client.prepareSearch(info.getTargetIndex())
        .setTypes(info.getTargetType())
        .setQuery(
                QueryBuilders.termQuery(info.getTargetIdField(),
                        targetId))
                        .addSort(info.getTimestampField(), SortOrder.DESC)
                        .setSize(info.getSize()).setFrom(info.getFrom())
                        .execute(on(responseListener, t -> onError(channel, t)));
    }

    private Map<String, Object> expandObjects(final Map<String, Object> source,
            final Info info) {
        final Map<String, Object> newSource = new HashMap<>(source.size());
        for (final Map.Entry<String, Object> entry : source.entrySet()) {
            final Object value = entry.getValue();
            if (info.getUserIdField().equals(entry.getKey()) && value != null) {
                final Number targetId = (Number) value;
                final Map<String, Object> objMap = getObjectMap("U-",
                        info.getUserIndex(), info.getUserType(),
                        targetId.toString());
                newSource.put("user", objMap);
            } else if (info.getItemIdField().equals(entry.getKey())
                    && value != null) {
                final Number targetId = (Number) value;
                final Map<String, Object> objMap = getObjectMap("I-",
                        info.getItemIndex(), info.getItemType(),
                        targetId.toString());
                newSource.put("item", objMap);
            } else if (value instanceof Map) {
                @SuppressWarnings("unchecked")
                final Map<String, Object> objMap = (Map<String, Object>) value;
                newSource.put(entry.getKey(), expandObjects(objMap, info));
            } else if (value instanceof List) {
                @SuppressWarnings("unchecked")
                final List<Object> list = (List<Object>) value;
                final List<Object> newList = new ArrayList<>(list.size());
                for (final Object obj : list) {
                    if (obj instanceof Map) {
                        @SuppressWarnings("unchecked")
                        final Map<String, Object> objMap = (Map<String, Object>) obj;
                        newList.add(expandObjects(objMap, info));
                    } else {
                        newList.add(obj);
                    }
                }
                newSource.put(entry.getKey(), newList);
            } else {
                newSource.put(entry.getKey(), value);
            }
        }
        return newSource;
    }

    private Map<String, Object> getObjectMap(final String prefix,
            final String index, final String type, final String id) {
        try {
            return cache.get(prefix + id, () -> {
                final GetResponse response = client.prepareGet(index, type, id)
                        .execute().actionGet();
                if (response.isExists()) {
                    return response.getSource();
                }
                return null;
            });
        } catch (final ExecutionException e) {
            throw new TasteException("Failed to get data for " + index + "/"
                    + type + "/" + id, e);
        }
    }

    private void onError(final RestChannel channel, final Throwable t) {
        try {
            channel.sendResponse(new BytesRestResponse(channel, t));
        } catch (final Exception e) {
            logger.error("Failed to send a failure response.", e);
        }
    }

    private static class Info {
        private static final String ITEM = "item";

        private static final String USER = "user";

        private String targetIndex;

        private String targetType;

        private String targetIdField;

        private String userIndex;

        private String userType;

        private String userIdField;

        private String itemIndex;

        private String itemType;

        private String itemIdField;

        private String idIndex;

        private String idType;

        private String idField;

        private String timestampField;

        private String objectType;

        private int size;

        private int from;

        Info(final RestRequest request) {
            size = request.paramAsInt("size", 10);
            from = request.paramAsInt("from", 0);
            targetIndex = request.param("index");
            targetType = request.param("type");
            userIndex = request.param(TasteConstants.REQUEST_PARAM_USER_INDEX,
                    targetIndex);
            userType = request.param(TasteConstants.REQUEST_PARAM_USER_TYPE,
                    TasteConstants.USER_TYPE);
            itemIndex = request.param(TasteConstants.REQUEST_PARAM_ITEM_INDEX,
                    targetIndex);
            itemType = request.param(TasteConstants.REQUEST_PARAM_ITEM_TYPE,
                    TasteConstants.ITEM_TYPE);
            userIdField = request.param(
                    TasteConstants.REQUEST_PARAM_USER_ID_FIELD,
                    TasteConstants.USER_ID_FIELD);
            itemIdField = request.param(
                    TasteConstants.REQUEST_PARAM_ITEM_ID_FIELD,
                    TasteConstants.ITEM_ID_FIELD);
            timestampField = request.param(
                    TasteConstants.REQUEST_PARAM_TIMESTAMP_FIELD,
                    TasteConstants.TIMESTAMP_FIELD);
            objectType = request.param("objectType");
            if (USER.equals(objectType)) {
                idIndex = userIndex;
                idType = userType;
                idField = request.param(TasteConstants.REQUEST_PARAM_ID_FIELD,
                        userIdField);
            } else if (ITEM.equals(objectType)) {
                idIndex = itemIndex;
                idType = itemType;
                idField = request.param(TasteConstants.REQUEST_PARAM_ID_FIELD,
                        itemIdField);
            }
            targetIdField = request.param(
                    TasteConstants.REQUEST_PARAM_TARGET_ID_FIELD, idField);
        }

        public int getFrom() {
            return from;
        }

        public int getSize() {
            return size;
        }

        public String getTargetIndex() {
            return targetIndex;
        }

        public String getTargetType() {
            return targetType;
        }

        public String getTargetIdField() {
            return targetIdField;
        }

        public String getUserIndex() {
            return userIndex;
        }

        public String getUserType() {
            return userType;
        }

        public String getItemIndex() {
            return itemIndex;
        }

        public String getItemType() {
            return itemType;
        }

        public String getUserIdField() {
            return userIdField;
        }

        public String getItemIdField() {
            return itemIdField;
        }

        public String getTimestampField() {
            return timestampField;
        }

        public String getIdIndex() {
            return idIndex;
        }

        public String getIdType() {
            return idType;
        }

        public String getIdField() {
            return idField;
        }

    }
}
