package org.codelibs.elasticsearch.taste;

import static org.codelibs.elasticsearch.runner.ElasticsearchClusterRunner.newConfigs;

import java.util.List;
import java.util.Map;

import org.codelibs.elasticsearch.runner.ElasticsearchClusterRunner;
import org.codelibs.elasticsearch.runner.net.Curl;
import org.codelibs.elasticsearch.runner.net.CurlResponse;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.client.Client;
import org.elasticsearch.common.settings.Settings.Builder;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.node.Node;
import org.elasticsearch.search.SearchHit;

import junit.framework.TestCase;

public class TastePluginTest extends TestCase {

    private ElasticsearchClusterRunner runner;

    private String clusterName;

    @Override
    protected void setUp() throws Exception {
        clusterName = "es-taste-" + System.currentTimeMillis();
        // create runner instance
        runner = new ElasticsearchClusterRunner();
        // create ES nodes
        runner.onBuild(new ElasticsearchClusterRunner.Builder() {
            @Override
            public void build(final int number, final Builder settingsBuilder) {
                settingsBuilder.put("http.cors.enabled", true);
                settingsBuilder.put("http.cors.allow-origin", "*");
                settingsBuilder.putArray("discovery.zen.ping.unicast.hosts", "localhost:9301-9305");
                settingsBuilder.put("plugin.types", "org.codelibs.elasticsearch.taste.TastePlugin");
            }
        }).build(newConfigs().clusterName(clusterName).numOfNode(1));

        // wait for yellow status
        runner.ensureYellow();
    }

    @Override
    protected void tearDown() throws Exception {
        // close runner
        runner.close();
        // delete all files
        runner.clean();
    }

    public void test_recommended_items_from_user() throws Exception {
        final Node node = runner.node();
        final Client client = runner.client();
        final String index = "test";

        int numOfUsers = 100;
        int numOfItems = 100;
        for (int i = 1; i < numOfUsers + 1; i++) {
            int inc = i % 10 + 1;
            for (int j = inc; j < numOfItems; j += inc + (i + j * 10) % 20) {
                int value = 1;// ((i % 5 + j % 5) % 2) * 4 + 1;
                String source = "{\"user\":{\"id\":" + i + "},\"item\":{\"id\":" + j + "},\"value\":" + value + ",\"timestamp\":"
                        + System.currentTimeMillis() + "}";
                runner.print(source);
                try (CurlResponse curlResponse = Curl.post(node, "/" + index + "/_taste/event").body(source).execute()) {
                    final String content = curlResponse.getContentAsString();
                    assertEquals("{\"acknowledged\":true}", content);
                }
            }
        }

        runner.refresh();
        assertEquals(994, client.prepareSearch(index).setSize(0).execute().actionGet().getHits().getTotalHits());

        String source =
                "{\"num_of_items\":10,\"data_model\":{\"cache\":{\"weight\":\"100m\"}},\"index_info\":{\"index\":\"" + index + "\"}}";
        try (CurlResponse curlResponse = Curl.post(node, "/_taste/action/recommended_items_from_user").body(source).execute()) {
            Map<String, Object> sourceMap = curlResponse.getContentAsMap();
            assertEquals("true", sourceMap.get("acknowledged").toString());
            assertNotNull(sourceMap.get("name"));
        }

        runner.refresh();
        for (int i = 0; i < 30; i++) {
            try (CurlResponse curlResponse = Curl.get(node, "/_taste/action").execute()) {
                Map<String, Object> sourceMap = curlResponse.getContentAsMap();
                assertEquals("true", sourceMap.get("acknowledged").toString());
                if (((List<?>) sourceMap.get("names")).isEmpty()) {
                    break;
                }
            }
            Thread.sleep(1000L);
        }

        runner.refresh();
        for (int i = 0; i < 30; i++) {
            long totalHits =
                    client.prepareSearch(index).setTypes("recommendation").setSize(0).execute().actionGet().getHits().getTotalHits();
            if (totalHits == 100) {
                break;
            }
            Thread.sleep(1000L);
        }
        assertEquals(100, client.prepareSearch(index).setTypes("recommendation").setSize(0).execute().actionGet().getHits().getTotalHits());

        {
            SearchResponse response = client.prepareSearch(index).setTypes("recommendation").setQuery(QueryBuilders.termQuery("user_id", 1))
                    .execute().actionGet();
            SearchHit[] hits = response.getHits().getHits();
            assertEquals(6, ((List<?>) hits[0].getSource().get("items")).size());
        }

        try (CurlResponse curlResponse = Curl.get(node, "/" + index + "/recommendation/_taste/user/1").execute()) {
            Map<String, Object> rootMap = curlResponse.getContentAsMap();
            Map<String, Object> hitsMap = (Map<String, Object>) rootMap.get("hits");
            List<?> hitsList = (List<?>) hitsMap.get("hits");
            Map<String, Object> sourceMap = (Map<String, Object>) ((Map<String, Object>) hitsList.get(0)).get("_source");
            List<?> itemsList = (List<?>) sourceMap.get("items");
            assertEquals(6, itemsList.size());
        }
    }

    public void test_recommended_items_from_item() throws Exception {
        final Node node = runner.node();
        final Client client = runner.client();
        final String index = "test";

        int numOfUsers = 100;
        int numOfItems = 100;
        for (int i = 1; i < numOfUsers + 1; i++) {
            int inc = i % 10 + 1;
            for (int j = inc; j < numOfItems; j += inc + (i + j * 10) % 20) {
                int value = 1;// ((i % 5 + j % 5) % 2) * 4 + 1;
                String source = "{\"user\":{\"id\":" + i + "},\"item\":{\"id\":" + j + "},\"value\":" + value + ",\"timestamp\":"
                        + System.currentTimeMillis() + "}";
                runner.print(source);
                try (CurlResponse curlResponse = Curl.post(node, "/" + index + "/_taste/event").body(source).execute()) {
                    final String content = curlResponse.getContentAsString();
                    assertEquals("{\"acknowledged\":true}", content);
                }
            }
        }

        runner.refresh();
        assertEquals(994, client.prepareSearch(index).setSize(0).execute().actionGet().getHits().getTotalHits());

        String source =
                "{\"num_of_items\":10,\"data_model\":{\"cache\":{\"weight\":\"100m\"}},\"index_info\":{\"index\":\"" + index + "\"}}";
        try (CurlResponse curlResponse = Curl.post(node, "/_taste/action/recommended_items_from_item").body(source).execute()) {
            Map<String, Object> sourceMap = curlResponse.getContentAsMap();
            assertEquals("true", sourceMap.get("acknowledged").toString());
            assertNotNull(sourceMap.get("name"));
        }

        runner.refresh();
        for (int i = 0; i < 30; i++) {
            try (CurlResponse curlResponse = Curl.get(node, "/_taste/action").execute()) {
                Map<String, Object> sourceMap = curlResponse.getContentAsMap();
                assertEquals("true", sourceMap.get("acknowledged").toString());
                if (((List<?>) sourceMap.get("names")).isEmpty()) {
                    break;
                }
            }
            Thread.sleep(1000L);
        }

        runner.refresh();
        for (int i = 0; i < 30; i++) {
            long totalHits =
                    client.prepareSearch(index).setTypes("item_similarity").setSize(0).execute().actionGet().getHits().getTotalHits();
            if (totalHits == 79) {
                break;
            }
            Thread.sleep(1000L);
        }
        assertEquals(79, client.prepareSearch(index).setTypes("item_similarity").setSize(0).execute().actionGet().getHits().getTotalHits());

        {
            SearchResponse response = client.prepareSearch(index).setTypes("item_similarity")
                    .setQuery(QueryBuilders.termQuery("item_id", 1)).execute().actionGet();
            SearchHit[] hits = response.getHits().getHits();
            assertEquals(10, ((List<?>) hits[0].getSource().get("items")).size());
        }
    }

    public void test_similar_users() throws Exception {
        final Node node = runner.node();
        final Client client = runner.client();
        final String index = "test";

        int numOfUsers = 100;
        int numOfItems = 100;
        for (int i = 1; i < numOfUsers + 1; i++) {
            int inc = i % 10 + 1;
            for (int j = inc; j < numOfItems; j += inc + (i + j * 10) % 20) {
                int value = 1;// ((i % 5 + j % 5) % 2) * 4 + 1;
                String source = "{\"user\":{\"id\":" + i + "},\"item\":{\"id\":" + j + "},\"value\":" + value + ",\"timestamp\":"
                        + System.currentTimeMillis() + "}";
                runner.print(source);
                try (CurlResponse curlResponse = Curl.post(node, "/" + index + "/_taste/event").body(source).execute()) {
                    final String content = curlResponse.getContentAsString();
                    assertEquals("{\"acknowledged\":true}", content);
                }
            }
        }

        runner.refresh();
        assertEquals(994, client.prepareSearch(index).setSize(0).execute().actionGet().getHits().getTotalHits());

        String source =
                "{\"num_of_items\":10,\"data_model\":{\"cache\":{\"weight\":\"100m\"}},\"index_info\":{\"index\":\"" + index + "\"}}";
        try (CurlResponse curlResponse = Curl.post(node, "/_taste/action/similar_users").body(source).execute()) {
            Map<String, Object> sourceMap = curlResponse.getContentAsMap();
            assertEquals("true", sourceMap.get("acknowledged").toString());
            assertNotNull(sourceMap.get("name"));
        }

        runner.refresh();
        for (int i = 0; i < 30; i++) {
            try (CurlResponse curlResponse = Curl.get(node, "/_taste/action").execute()) {
                Map<String, Object> sourceMap = curlResponse.getContentAsMap();
                assertEquals("true", sourceMap.get("acknowledged").toString());
                if (((List<?>) sourceMap.get("names")).isEmpty()) {
                    break;
                }
            }
            Thread.sleep(1000L);
        }

        runner.refresh();
        for (int i = 0; i < 30; i++) {
            long totalHits =
                    client.prepareSearch(index).setTypes("user_similarity").setSize(0).execute().actionGet().getHits().getTotalHits();
            if (totalHits == 100) {
                break;
            }
            Thread.sleep(1000L);
        }
        assertEquals(100,
                client.prepareSearch(index).setTypes("user_similarity").setSize(0).execute().actionGet().getHits().getTotalHits());

        {
            SearchResponse response = client.prepareSearch(index).setTypes("user_similarity")
                    .setQuery(QueryBuilders.termQuery("user_id", 1)).execute().actionGet();
            SearchHit[] hits = response.getHits().getHits();
            assertEquals(10, ((List<?>) hits[0].getSource().get("users")).size());
        }
    }

    public void test_evaluate_items_from_user() throws Exception {
        final Node node = runner.node();
        final Client client = runner.client();
        final String index = "test";

        int numOfUsers = 100;
        int numOfItems = 100;
        for (int i = 1; i < numOfUsers + 1; i++) {
            int inc = i % 10 + 1;
            for (int j = inc; j < numOfItems; j += inc + (i + j * 10) % 20) {
                int value = 1;// ((i % 5 + j % 5) % 2) * 4 + 1;
                String source = "{\"user\":{\"id\":" + i + "},\"item\":{\"id\":" + j + "},\"value\":" + value + ",\"timestamp\":"
                        + System.currentTimeMillis() + "}";
                runner.print(source);
                try (CurlResponse curlResponse = Curl.post(node, "/" + index + "/_taste/event").body(source).execute()) {
                    final String content = curlResponse.getContentAsString();
                    assertEquals("{\"acknowledged\":true}", content);
                }
            }
        }

        runner.refresh();
        assertEquals(994, client.prepareSearch(index).setSize(0).execute().actionGet().getHits().getTotalHits());

        String source =
                "{\"evaluation_percentage\":1.0,\"training_percentage\":0.9,\"margin_for_error\":1.0,\"data_model\":{\"cache\":{\"weight\":\"100m\"}},\"index_info\":{\"index\":\""
                        + index
                        + "\"},\"neighborhood\":{\"factory\":\"org.codelibs.elasticsearch.taste.neighborhood.NearestNUserNeighborhoodFactory\",\"neighborhood_size\":100},\"evaluator\":{\"id\":\"test_result\",\"factory\":\"org.codelibs.elasticsearch.taste.eval.RMSEvaluatorFactory\"}}";
        try (CurlResponse curlResponse = Curl.post(node, "/_taste/action/evaluate_items_from_user").body(source).execute()) {
            Map<String, Object> sourceMap = curlResponse.getContentAsMap();
            assertEquals("true", sourceMap.get("acknowledged").toString());
            assertNotNull(sourceMap.get("name"));
        }

        runner.refresh();
        for (int i = 0; i < 30; i++) {
            try (CurlResponse curlResponse = Curl.get(node, "/_taste/action").execute()) {
                Map<String, Object> sourceMap = curlResponse.getContentAsMap();
                assertEquals("true", sourceMap.get("acknowledged").toString());
                if (((List<?>) sourceMap.get("names")).isEmpty()) {
                    break;
                }
            }
            Thread.sleep(1000L);
        }

        runner.refresh();
        for (int i = 0; i < 30; i++) {
            long totalHits = client.prepareSearch(index).setTypes("report").setSize(0).execute().actionGet().getHits().getTotalHits();
            if (totalHits == 1) {
                break;
            }
            Thread.sleep(1000L);
        }
        assertEquals(1, client.prepareSearch(index).setTypes("report").setSize(0).execute().actionGet().getHits().getTotalHits());

        {
            SearchResponse response = client.prepareSearch(index).setTypes("report")
                    .setQuery(QueryBuilders.termQuery("evaluator_id", "test_result")).execute().actionGet();
            SearchHit[] hits = response.getHits().getHits();
            assertEquals(1, hits.length);
        }
    }
}
