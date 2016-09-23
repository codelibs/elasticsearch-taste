package org.codelibs.elasticsearch.taste.writer;

import java.io.Closeable;
import java.io.IOException;

import org.codelibs.elasticsearch.taste.TasteConstants;
import org.codelibs.elasticsearch.taste.exception.TasteException;
import org.elasticsearch.action.admin.indices.create.CreateIndexResponse;
import org.elasticsearch.action.admin.indices.exists.indices.IndicesExistsResponse;
import org.elasticsearch.action.admin.indices.mapping.get.GetMappingsResponse;
import org.elasticsearch.action.admin.indices.mapping.put.PutMappingResponse;
import org.elasticsearch.client.Client;
import org.elasticsearch.common.xcontent.XContentBuilder;

public abstract class AbstractWriter implements Closeable {

    protected Client client;

    protected String index;

    protected String type;

    protected String timestampField = TasteConstants.TIMESTAMP_FIELD;

    protected XContentBuilder mappingBuilder;

    public AbstractWriter(final Client client, final String index,
            final String type) {
        this.client = client;
        this.index = index;
        this.type = type;
    }

    public void open() {
        final IndicesExistsResponse existsResponse = client.admin().indices()
                .prepareExists(index).execute().actionGet();
        if (!existsResponse.isExists()) {
            final CreateIndexResponse createIndexResponse = client.admin()
                    .indices().prepareCreate(index).execute().actionGet();
            if (!createIndexResponse.isAcknowledged()) {
                throw new TasteException("Failed to create " + index
                        + " index.");
            }
        }

        if (mappingBuilder != null) {
            final GetMappingsResponse response = client.admin().indices()
                    .prepareGetMappings(index).setTypes(type).execute()
                    .actionGet();
            if (response.mappings().isEmpty()) {
                final PutMappingResponse putMappingResponse = client.admin()
                        .indices().preparePutMapping(index).setType(type)
                        .setSource(mappingBuilder).execute().actionGet();
                if (!putMappingResponse.isAcknowledged()) {
                    throw new TasteException("Failed to create a mapping of"
                            + index + "/" + type);
                }
            }
        }
    }

    @Override
    public abstract void close() throws IOException;

    public void setTimestampField(final String timestampField) {
        this.timestampField = timestampField;
    }

    public void setMapping(final XContentBuilder builder) {
        mappingBuilder = builder;
    }

}