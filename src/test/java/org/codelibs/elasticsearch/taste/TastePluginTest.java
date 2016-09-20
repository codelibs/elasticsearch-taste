package org.codelibs.elasticsearch.taste;

import static org.codelibs.elasticsearch.runner.ElasticsearchClusterRunner.newConfigs;

import org.codelibs.elasticsearch.runner.ElasticsearchClusterRunner;
import org.codelibs.elasticsearch.runner.net.Curl;
import org.codelibs.elasticsearch.runner.net.CurlResponse;
import org.elasticsearch.client.Client;
import org.elasticsearch.common.settings.Settings.Builder;
import org.elasticsearch.node.Node;

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

    public void test_event() throws Exception {
        final Node node = runner.node();
        final Client client = runner.client();
        final String index = "test";

        int numOfUsers = 100;
        int numOfItems = 100;
        for (int i = 1; i < numOfUsers + 1; i++) {
            int inc = numOfUsers % 10 + (9 - numOfUsers % 9) + 1;
            for (int j = inc; j < numOfItems; j += inc + 10) {
                int value = (i % 5 + j % 5) % 5 + 1;
                final String source = "{\"user\":{\"id\":" + i + "},\"item\":{\"id\":" + j + "},\"value\":" + value + ",\"timestamp\":"
                        + System.currentTimeMillis() + "}";
                System.out.println(source);
                try (CurlResponse curlResponse = Curl.post(node, "/" + index + "/_taste/event").body(source).execute()) {
                    final String content = curlResponse.getContentAsString();
                    assertEquals("{\"acknowledged\":true}", content);
                }
            }
        }

        runner.refresh();
        assertEquals(605, client.prepareSearch(index).setSize(0).execute().actionGet().getHits().getTotalHits());
    }
}
