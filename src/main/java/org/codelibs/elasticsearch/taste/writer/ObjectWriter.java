package org.codelibs.elasticsearch.taste.writer;

import java.util.Date;
import java.util.Map;

import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.index.IndexResponse;
import org.elasticsearch.client.Client;
import org.elasticsearch.common.logging.ESLogger;
import org.elasticsearch.common.logging.Loggers;

public class ObjectWriter extends AbstractWriter {
    private static final ESLogger logger = Loggers.getLogger(ItemWriter.class);

    public ObjectWriter(final Client client, final String index,
            final String type) {
        super(client, index, type);
    }

    public void write(final Map<String, Object> rootObj) {
        rootObj.put(timestampField, new Date());

        client.prepareIndex(index, type).setSource(rootObj)
        .execute(new ActionListener<IndexResponse>() {

            @Override
            public void onResponse(final IndexResponse response) {
                if (logger.isDebugEnabled()) {
                    logger.debug(
                            "Response: {}/{}/{}, Created: {}, Version: {}",
                            response.getIndex(), response.getType(),
                            response.getId(), response.getVersion(),
                            response.isCreated());
                }
            }

            @Override
            public void onFailure(final Throwable e) {
                logger.error("Failed to write " + rootObj, e);
            }
        });
    }

}
