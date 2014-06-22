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
import java.util.regex.Pattern;

import org.codelibs.elasticsearch.taste.TasteConstants;
import org.codelibs.elasticsearch.taste.TasteSystemException;
import org.codelibs.elasticsearch.taste.exception.NotFoundException;
import org.codelibs.elasticsearch.taste.exception.OperationFailedException;
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
    private static Pattern idPattern = Pattern.compile("[\\._]id",
            Pattern.CASE_INSENSITIVE);

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
                "/{index}/_taste/{field}/{id}", this);
        restController.registerHandler(RestRequest.Method.GET,
                "/{index}/{type}/_taste/{field}/{id}", this);

    }

    @Override
    public void handleRequest(final RestRequest request,
            final RestChannel channel) {

        final String index = request
                .param(TasteConstants.REQUEST_PARAM_INFO_INDEX,
                        request.param("index"));
        final String type = request
                .param(TasteConstants.REQUEST_PARAM_INFO_TYPE);
        final String timestampField = request.param(
                TasteConstants.REQUEST_PARAM_TIMESTAMP_FIELD,
                TasteConstants.TIMESTAMP_FIELD);

        final String id = request.param("id");
        if (StringUtils.isBlank(id)) {
            onError(channel, new NotFoundException("No id."));
            return;
        }

        final String fieldName = request.param("field");
        if (StringUtils.isBlank(fieldName)) {
            onError(channel, new NotFoundException("No field."));
            return;
        }

        final OnResponseListener<SearchResponse> responseListener = searchResponse -> {
            final SearchHits hits = searchResponse.getHits();
            if (hits.totalHits() == 0) {
                onError(channel, new NotFoundException("No " + fieldName
                        + " data for " + id + " in " + index + "/" + type));
                return;
            }
            final SearchHit hit = hits.getHits()[0];
            final SearchHitField field = hit.field(fieldName);
            final Number targetId = field.getValue();
            if (targetId == null) {
                onError(channel, new NotFoundException("No " + fieldName
                        + " for " + id + " in " + index + "/" + type));
                return;
            }

            doSearchRequest(request, channel, fieldName, targetId.longValue());
        };
        client.prepareSearch(index).setTypes(type)
                .setQuery(QueryBuilders.termQuery("id", id))
                .addField(fieldName).addSort(timestampField, SortOrder.DESC)
                .execute(on(responseListener, t -> onError(channel, t)));

    }

    private void doSearchRequest(final RestRequest request,
            final RestChannel channel, final String fieldName,
            final long targetId) {
        final String index = request.param("index");
        final String type = request.param("type");
        final String timestampField = request.param(
                TasteConstants.REQUEST_PARAM_TIMESTAMP_FIELD,
                TasteConstants.TIMESTAMP_FIELD);

        final OnResponseListener<SearchResponse> responseListener = response -> {
            final SearchHits hits = response.getHits();
            if (hits.totalHits() == 0) {
                onError(channel, new NotFoundException("No ID for " + targetId
                        + " in " + index + "/" + type));
                return;
            }

            final String infoIndex = request.param(
                    TasteConstants.REQUEST_PARAM_INFO_INDEX,
                    request.param("index"));
            final String infoType = request
                    .param(TasteConstants.REQUEST_PARAM_INFO_TYPE);

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

                final String fieldObjectName = getFieldObjectName(fieldName);
                for (final SearchHit hit : hits.getHits()) {
                    final Map<String, Object> source = expandInfoObject(
                            hit.getSource(), infoIndex, infoType, fieldName,
                            fieldObjectName);
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
        client.prepareSearch(index).setTypes(type)
                .setQuery(QueryBuilders.termQuery(fieldName, targetId))
                .addSort(timestampField, SortOrder.DESC)
                .execute(on(responseListener, t -> onError(channel, t)));
    }

    private String getFieldObjectName(final String fieldName) {
        return idPattern.matcher(fieldName).replaceFirst(
                StringUtils.EMPTY_STRING);
    }

    private Map<String, Object> expandInfoObject(
            final Map<String, Object> source, final String index,
            final String type, final String fieldName,
            final String fieldObjectName) {
        final Map<String, Object> newSource = new HashMap<>(source.size());
        for (final Map.Entry<String, Object> entry : source.entrySet()) {
            final Object value = entry.getValue();
            if (fieldName.equals(entry.getKey()) && value != null) {
                final Number targetId = (Number) value;
                final Map<String, Object> objMap = getInfoObjectMap(index,
                        type, targetId.toString());
                newSource.put(fieldObjectName, objMap);
            } else if (value instanceof Map) {
                @SuppressWarnings("unchecked")
                final Map<String, Object> objMap = (Map<String, Object>) value;
                newSource.put(
                        entry.getKey(),
                        expandInfoObject(objMap, index, type, fieldName,
                                fieldObjectName));
            } else if (value instanceof List) {
                @SuppressWarnings("unchecked")
                final List<Object> list = (List<Object>) value;
                final List<Object> newList = new ArrayList<>(list.size());
                for (final Object obj : list) {
                    if (obj instanceof Map) {
                        @SuppressWarnings("unchecked")
                        final Map<String, Object> objMap = (Map<String, Object>) obj;
                        newList.add(expandInfoObject(objMap, index, type,
                                fieldName, fieldObjectName));
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

    private Map<String, Object> getInfoObjectMap(final String index,
            final String type, final String id) {
        try {
            return cache.get(id, () -> {
                final GetResponse response = client.prepareGet(index, type, id)
                        .execute().actionGet();
                if (response.isExists()) {
                    return response.getSource();
                }
                return null;
            });
        } catch (final ExecutionException e) {
            throw new TasteSystemException("Failed to get data for " + index
                    + "/" + type + "/" + id, e);
        }
    }

    private void onError(final RestChannel channel, final Throwable t) {
        try {
            channel.sendResponse(new BytesRestResponse(channel, t));
        } catch (final Exception e) {
            logger.error("Failed to send a failure response.", e);
        }
    }

}
