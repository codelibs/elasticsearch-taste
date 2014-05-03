package org.codelibs.elasticsearch.taste.writer;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.Queue;
import java.util.concurrent.ConcurrentLinkedQueue;

import org.codelibs.elasticsearch.taste.TasteConstants;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.bulk.BulkRequestBuilder;
import org.elasticsearch.action.bulk.BulkResponse;
import org.elasticsearch.client.Client;
import org.elasticsearch.common.logging.ESLogger;
import org.elasticsearch.common.logging.Loggers;

public class ResultWriter extends AbstractWriter {
    private static final ESLogger logger = Loggers
            .getLogger(ResultWriter.class);

    protected volatile Queue<Map<String, Object>> resultQueue = new ConcurrentLinkedQueue<>();

    protected String userIdField = TasteConstants.USER_ID_FIELD;

    protected String itemIdField = TasteConstants.ITEM_ID_FIELD;

    protected int maxQueueSize = 1000;

    public ResultWriter(final Client client, final String index,
            final String type) {
        super(client, index, type);
    }

    @Override
    public void close() throws IOException {
        flush();
    }

    public void write(final String evaluatorId, final long userID,
            final long itemID, final String resultType, final float actual,
            final float estimate, final long time) {
        final Map<String, Object> rootObj = new HashMap<>();

        rootObj.put("result_type", resultType);
        rootObj.put("evaluator_id", evaluatorId);
        rootObj.put(userIdField, userID);
        rootObj.put(itemIdField, itemID);
        rootObj.put("actual", actual);
        if (!Float.isNaN(estimate)) {
            rootObj.put("estimate", estimate);
        }
        rootObj.put("computing_time", time);

        resultQueue.add(rootObj);

        if (resultQueue.size() > maxQueueSize) {
            flush();
        }
    }

    protected synchronized void flush() {
        if (resultQueue.isEmpty()) {
            return;
        }

        final Queue<Map<String, Object>> currentQueue = resultQueue;
        resultQueue = new ConcurrentLinkedQueue<>();

        final BulkRequestBuilder bulkRequest = client.prepareBulk();

        for (final Map<String, Object> obj : currentQueue) {
            bulkRequest.add(client.prepareIndex(index, type).setSource(obj));
        }

        bulkRequest.execute(new ActionListener<BulkResponse>() {
            @Override
            public void onResponse(final BulkResponse response) {
                if (response.hasFailures()) {
                    logger.error("Failed to write a result on {}/{}: {}",
                            index, type, response.buildFailureMessage());
                } else {
                    logger.info("Wrote {} results in {}/{}.",
                            currentQueue.size(), index, type);
                }
            }

            @Override
            public void onFailure(final Throwable e) {
                logger.error("Failed to write a result on {}/{}.", e, index,
                        type);
            }
        });
    }

    public void setUserIdField(final String userIdField) {
        this.userIdField = userIdField;
    }

    public void setItemIdField(final String itemIdField) {
        this.itemIdField = itemIdField;
    }

    public void setMaxQueueSize(final int maxQueueSize) {
        this.maxQueueSize = maxQueueSize;
    }
}
