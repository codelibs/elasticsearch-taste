package org.codelibs.elasticsearch.taste.writer;

import java.io.Closeable;
import java.io.IOException;
import java.util.Date;
import java.util.Map;

import org.codelibs.elasticsearch.taste.TasteConstants;
import org.codelibs.elasticsearch.taste.TasteSystemException;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.admin.indices.mapping.get.GetMappingsResponse;
import org.elasticsearch.action.admin.indices.mapping.put.PutMappingResponse;
import org.elasticsearch.action.index.IndexResponse;
import org.elasticsearch.client.Client;
import org.elasticsearch.common.logging.ESLogger;
import org.elasticsearch.common.logging.Loggers;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentFactory;

public class ReportWriter implements Closeable {
    private static final ESLogger logger = Loggers
            .getLogger(RecommendedItemsWriter.class);

    protected Client client;

    protected String index;

    protected String type = TasteConstants.REPORT_TYPE;

    protected String timestampField = TasteConstants.TIMESTAMP_FIELD;

    public ReportWriter(final Client client, final String index) {
        this.client = client;
        this.index = index;
    }

    public void open() {
        final GetMappingsResponse response = client.admin().indices()
                .prepareGetMappings(index).setTypes(type).execute().actionGet();
        if (response.mappings().isEmpty()) {
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

                        // value
                        .startObject("report_type")//
                        .field("type", "string")//
                        .field("index", "not_analyzed")//
                        .endObject()//

                        .endObject()//
                        .endObject()//
                        .endObject();

                final PutMappingResponse putMappingResponse = client.admin()
                        .indices().preparePutMapping(index).setType(type)
                        .setSource(builder).execute().actionGet();
                if (!putMappingResponse.isAcknowledged()) {
                    throw new TasteSystemException(
                            "Failed to create a mapping of" + index + "/"
                                    + type);
                }
            } catch (final IOException e) {
                throw new TasteSystemException("Failed to create a mapping of"
                        + index + "/" + type, e);
            }
        }
    }

    @Override
    public void close() throws IOException {
        // nothing
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

    public void setType(final String type) {
        this.type = type;
    }

    public void setTimestampField(final String timestampField) {
        this.timestampField = timestampField;
    }

}
