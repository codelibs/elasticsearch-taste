package org.codelibs.elasticsearch.taste.util;

import java.util.List;

import org.elasticsearch.ElasticsearchException;
import org.elasticsearch.action.admin.cluster.health.ClusterHealthResponse;
import org.elasticsearch.client.Client;

public final class ClusterUtils {
    private ClusterUtils() {
    }

    public static void waitForAvailable(final Client client,
            final String... indices) {
        final ClusterHealthResponse response = client.admin().cluster()
                .prepareHealth(indices).setWaitForYellowStatus().execute()
                .actionGet();
        final List<String> failures = response.getValidationFailures();
        if (!failures.isEmpty()) {
            throw new ElasticsearchException(
                    "Cluster is not available: " + failures.toString());
        }
    }
}
