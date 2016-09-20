package org.codelibs.elasticsearch.taste;

import static org.codelibs.elasticsearch.runner.ElasticsearchClusterRunner.newConfigs;

import org.codelibs.elasticsearch.runner.ElasticsearchClusterRunner;
import org.codelibs.elasticsearch.runner.net.Curl;
import org.codelibs.elasticsearch.runner.net.CurlResponse;
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
                settingsBuilder.putArray("discovery.zen.ping.unicast.hosts",
                        "localhost:9301-9305");
                settingsBuilder.put("plugin.types",
                        "org.codelibs.elasticsearch.taste.TastePlugin");
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

        try (CurlResponse curlResponse = Curl
                .post(node, "/movielens/_taste/event")
                .body("{\"user\":{\"id\":263},\"item\":{\"id\":1451},\"value\":4,\"timestamp\":891299949000}")
                .execute()) {
            final String content = curlResponse.getContentAsString();
            assertEquals("{\"acknowledged\":true}", content);
        }

    }
}
