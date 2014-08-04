package org.codelibs.elasticsearch.taste.model;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.fail;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.OutputStreamWriter;
import java.io.Writer;
import java.util.Date;

import org.codelibs.elasticsearch.taste.TasteConstants;
import org.codelibs.elasticsearch.taste.common.FastIDSet;
import org.codelibs.elasticsearch.taste.common.LongPrimitiveIterator;
import org.codelibs.elasticsearch.taste.exception.TasteException;
import org.elasticsearch.action.admin.indices.create.CreateIndexResponse;
import org.elasticsearch.action.admin.indices.mapping.put.PutMappingResponse;
import org.elasticsearch.client.Client;
import org.elasticsearch.common.io.FileSystemUtils;
import org.elasticsearch.common.network.NetworkUtils;
import org.elasticsearch.common.settings.ImmutableSettings;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentFactory;
import org.elasticsearch.node.Node;
import org.elasticsearch.node.NodeBuilder;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import com.google.common.base.Charsets;

public class ElasticsearchDataModelTest {
    private static final String TEST_INDEX = "test";

    private static final String[] DATA = {//
    "123,456,0.1",//
            "123,789,0.6",//
            "123,654,0.7",//
            "234,123,0.5",//
            "234,234,1.0",//
            "234,999,0.9",//
            "345,789,0.6",//
            "345,654,0.7",//
            "345,123,1.0",//
            "345,234,0.5",//
            "345,999,0.5",//
            "456,456,0.1",//
            "456,789,0.5",//
            "456,654,0.0",//
            "456,999,0.2",//
    };

    private static final long[] USER_IDS = { 123, 234, 345, 456 };

    private static final long[] ITEM_IDS = { 123, 234, 456, 654, 789, 999 };

    private Node node;

    @Before
    public void setup() throws Exception {
        final ImmutableSettings.Builder builder = ImmutableSettings
                .settingsBuilder()
                .put("node.name", "node-test-" + System.currentTimeMillis())
                .put("node.data", true)
                .put("cluster.name",
                        "cluster-test-"
                                + NetworkUtils.getLocalAddress().getHostName())
                .put("index.store.type", "memory")
                .put("index.store.fs.memory.enabled", "true")
                .put("gateway.type", "none")
                .put("path.data", "./target/elasticsearch-test/data")
                .put("path.work", "./target/elasticsearch-test/work")
                .put("path.logs", "./target/elasticsearch-test/logs")
                .put("index.number_of_shards", "1")
                .put("index.number_of_replicas", "0")
                .put("cluster.routing.schedule", "50ms")
                .put("node.local", true);
        node = NodeBuilder.nodeBuilder().settings(builder).node();
        final Client client = node.client();

        // Wait for Yellow status
        client.admin().cluster().prepareHealth().setWaitForYellowStatus()
                .setTimeout(TimeValue.timeValueMinutes(1)).execute()
                .actionGet();

        final CreateIndexResponse response = client.admin().indices()
                .prepareCreate(TEST_INDEX).execute().actionGet();
        if (!response.isAcknowledged()) {
            throw new TasteException("Failed to create index: " + TEST_INDEX);
        }

        final XContentBuilder userBuilder = XContentFactory.jsonBuilder()//
                .startObject()//
                .startObject(TasteConstants.USER_TYPE)//
                .startObject("properties")//

                // @timestamp
                .startObject(TasteConstants.TIMESTAMP_FIELD)//
                .field("type", "date")//
                .field("format", "dateOptionalTime")//
                .endObject()//

                // user_id
                .startObject(TasteConstants.USER_ID_FIELD)//
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

        final PutMappingResponse userResponse = client.admin().indices()
                .preparePutMapping(TEST_INDEX)
                .setType(TasteConstants.USER_TYPE).setSource(userBuilder)
                .execute().actionGet();
        if (!userResponse.isAcknowledged()) {
            throw new TasteException("Failed to create user mapping.");
        }

        final XContentBuilder itemBuilder = XContentFactory.jsonBuilder()//
                .startObject()//
                .startObject(TasteConstants.ITEM_TYPE)//
                .startObject("properties")//

                // @timestamp
                .startObject(TasteConstants.TIMESTAMP_FIELD)//
                .field("type", "date")//
                .field("format", "dateOptionalTime")//
                .endObject()//

                // item_id
                .startObject(TasteConstants.ITEM_ID_FIELD)//
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

        final PutMappingResponse itemResponse = client.admin().indices()
                .preparePutMapping(TEST_INDEX)
                .setType(TasteConstants.ITEM_TYPE).setSource(itemBuilder)
                .execute().actionGet();
        if (!itemResponse.isAcknowledged()) {
            throw new TasteException("Failed to create item mapping.");
        }
    }

    @After
    public void tearDown() {
        node.close();

        FileSystemUtils.deleteRecursively(new File(
                "./target/elasticsearch-test/"), true);
    }

    @Test
    public void compareModel() throws Exception {
        final ElasticsearchDataModel esModel = getElasticsearchDataModel(DATA);
        final FileDataModel fsModel = getFileDataModel(DATA);

        compare(esModel, fsModel);

    }

    @Test
    public void compareModelWithCache() throws Exception {
        final ElasticsearchDataModel esModel = getElasticsearchDataModel(DATA);
        esModel.setMaxCacheWeight(1000000);

        for (int i = 0; i < 3; i++) {
            final FileDataModel fsModel = getFileDataModel(DATA);
            compare(esModel, fsModel);
        }

    }

    private void compare(final ElasticsearchDataModel esModel,
            final FileDataModel fsModel) {
        assertLongPrimitiveIterator(fsModel.getUserIDs(), esModel.getUserIDs());
        assertLongPrimitiveIterator(fsModel.getItemIDs(), esModel.getItemIDs());
        assertEquals(fsModel.getNumUsers(), esModel.getNumUsers());
        assertEquals(fsModel.getNumItems(), esModel.getNumItems());
        assertEquals(fsModel.getMaxPreference(), esModel.getMaxPreference(), 0);
        assertEquals(fsModel.getMinPreference(), esModel.getMinPreference(), 0);
        for (final long itemID1 : ITEM_IDS) {
            for (final long itemID2 : ITEM_IDS) {
                if (itemID1 == itemID2) {
                    continue;
                }
                assertEquals(
                        fsModel.getNumUsersWithPreferenceFor(itemID1, itemID2),
                        esModel.getNumUsersWithPreferenceFor(itemID1, itemID2));
            }
        }
        for (final long userID : USER_IDS) {
            assertFastIDSet(fsModel.getItemIDsFromUser(userID),
                    esModel.getItemIDsFromUser(userID));
            assertPreferenceArray(fsModel.getPreferencesFromUser(userID),
                    esModel.getPreferencesFromUser(userID));
        }
        for (final long itemID : ITEM_IDS) {
            assertEquals(fsModel.getNumUsersWithPreferenceFor(itemID),
                    esModel.getNumUsersWithPreferenceFor(itemID));
            assertPreferenceArray(fsModel.getPreferencesForItem(itemID),
                    esModel.getPreferencesForItem(itemID));
        }
        for (final long userID : USER_IDS) {
            for (final long itemID : ITEM_IDS) {
                assertEquals(fsModel.getPreferenceValue(userID, itemID),
                        esModel.getPreferenceValue(userID, itemID));
            }
        }
    }

    private void assertPreferenceArray(final PreferenceArray expected,
            final PreferenceArray actual) {
        assertEquals(expected.length(), actual.length());
        expected.sortByValue();
        actual.sortByValue();
        for (int i = 0; i < expected.length(); i++) {
            assertEquals(expected.getUserID(i), actual.getUserID(i));
            assertEquals(expected.getItemID(i), actual.getItemID(i));
            assertEquals(expected.getValue(i), actual.getValue(i), 0);
        }
    }

    private void assertFastIDSet(final FastIDSet expected,
            final FastIDSet actual) {
        assertLongPrimitiveIterator(expected.iterator(), actual.iterator());
    }

    private void assertLongPrimitiveIterator(
            final LongPrimitiveIterator expected,
            final LongPrimitiveIterator actual) {
        boolean loop = false;
        do {
            final boolean hasNext1 = expected.hasNext();
            final boolean hasNext2 = actual.hasNext();
            if (hasNext1 != hasNext2) {
                fail(hasNext1 + "!=" + hasNext2);
            }
            loop = hasNext1;
            if (loop) {
                final Long next1 = expected.next();
                final Long next2 = actual.next();
                assertEquals(next1, next2);
            }
        } while (loop);
    }

    private ElasticsearchDataModel getElasticsearchDataModel(
            final String[] lines) {
        final ElasticsearchDataModel esModel = new ElasticsearchDataModel();
        esModel.setClient(node.client());
        esModel.setItemIndex(TEST_INDEX);
        esModel.setUserIndex(TEST_INDEX);
        esModel.setPreferenceIndex(TEST_INDEX);
        for (final String line : lines) {
            final String[] values = line.split(",");
            esModel.setPreference(Long.parseLong(values[0]),
                    Long.parseLong(values[1]), Float.parseFloat(values[2]));
        }
        esModel.setLastAccessed(new Date());
        return esModel;
    }

    private FileDataModel getFileDataModel(final String[] lines)
            throws IOException {
        final File f = File.createTempFile("taste", ".csv");
        f.deleteOnExit();
        try (Writer writer = new OutputStreamWriter(new FileOutputStream(f),
                Charsets.UTF_8)) {
            for (final String line : lines) {
                writer.write(line);
                writer.write('\n');
            }
        }
        return new FileDataModel(f);
    }
}
