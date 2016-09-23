package org.codelibs.elasticsearch.taste.writer;

import java.util.Date;
import java.util.Map;
import java.util.concurrent.ArrayBlockingQueue;

import org.codelibs.elasticsearch.taste.exception.TasteException;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.index.IndexResponse;
import org.elasticsearch.client.Client;
import org.elasticsearch.common.logging.ESLogger;
import org.elasticsearch.common.logging.Loggers;

public class ObjectWriter extends AbstractWriter {
    private static final ESLogger logger = Loggers.getLogger(ItemWriter.class);

    private ArrayBlockingQueue<Boolean> queue;

    public ObjectWriter(final Client client, final String index,
            final String type, final int capacity) {
        super(client, index, type);
        queue = new ArrayBlockingQueue<>(capacity);
    }

    public void write(final Map<String, Object> rootObj) {
        countUp();
        try {
            rootObj.put(timestampField, new Date());

            client.prepareIndex(index, type).setSource(rootObj)
                    .execute(new ActionListener<IndexResponse>() {
                        @Override
                        public void onResponse(final IndexResponse response) {
                            if (logger.isDebugEnabled()) {
                                logger.debug(
                                        "Response: {}/{}/{}, Created: {}, Version: {}",
                                        response.getIndex(), response.getType(),
                                        response.getId(), response.isCreated(),
                                        response.getVersion());
                            }
                            countDown();
                        }

                        @Override
                        public void onFailure(final Throwable e) {
                            logger.error("Failed to write " + rootObj, e);
                            countDown();
                        }
                    });
        } catch (final Throwable t) {
            countDown();
            throw new TasteException(t);
        }
    }

    protected void countUp() {
        try {
            queue.put(Boolean.TRUE);
        } catch (InterruptedException e) {
            throw new TasteException(e);
        }
    }

    protected void countDown() {
        queue.poll();
    }
}
