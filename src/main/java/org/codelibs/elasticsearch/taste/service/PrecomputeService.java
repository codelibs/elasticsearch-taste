package org.codelibs.elasticsearch.taste.service;

import java.io.IOException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

import org.apache.commons.io.IOUtils;
import org.apache.mahout.cf.taste.common.TasteException;
import org.apache.mahout.cf.taste.impl.common.LongPrimitiveIterator;
import org.apache.mahout.cf.taste.model.DataModel;
import org.apache.mahout.cf.taste.recommender.ItemBasedRecommender;
import org.apache.mahout.cf.taste.similarity.precompute.SimilarItemsWriter;
import org.codelibs.elasticsearch.taste.similarity.worker.SimilarItemsWorker;
import org.elasticsearch.ElasticsearchException;
import org.elasticsearch.common.component.AbstractLifecycleComponent;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.settings.Settings;

public class PrecomputeService extends
        AbstractLifecycleComponent<PrecomputeService> {

    @Inject
    public PrecomputeService(final Settings settings) {
        super(settings);
        logger.info("CREATE PrecomputeService");

        // TODO Your code..
    }

    @Override
    protected void doStart() throws ElasticsearchException {
        logger.info("START PrecomputeService");

        // TODO Your code..
    }

    @Override
    protected void doStop() throws ElasticsearchException {
        logger.info("STOP PrecomputeService");

        // TODO Your code..
    }

    @Override
    protected void doClose() throws ElasticsearchException {
        logger.info("CLOSE PrecomputeService");

        // TODO Your code..
    }

    public void compute(final ItemBasedRecommender recommender,
            final SimilarItemsWriter writer, final int numOfMostSimilarItems,
            final int degreeOfParallelism, final int maxDurationInHours) {

        final ExecutorService executorService = Executors
                .newFixedThreadPool(degreeOfParallelism + 1);
        try {
            writer.open();

            final DataModel dataModel = recommender.getDataModel();
            final LongPrimitiveIterator itemIDs = dataModel.getItemIDs();

            for (int n = 0; n < degreeOfParallelism; n++) {
                executorService.execute(new SimilarItemsWorker(n, recommender,
                        itemIDs, numOfMostSimilarItems, writer));
            }
        } catch (TasteException | IOException e) {
            logger.error("Recommender {} is failed.", e, recommender);
        } finally {
            executorService.shutdown();
            boolean succeeded = false;
            try {
                succeeded = executorService.awaitTermination(
                        maxDurationInHours, TimeUnit.HOURS);
                if (!succeeded) {
                    logger.warn(
                            "Unable to complete the computation in {} hours!",
                            maxDurationInHours);
                }
            } catch (final InterruptedException e) {
                logger.warn("Interrupted a executor.", e);
            }
            if (!succeeded) {
                executorService.shutdownNow();
            }

            IOUtils.closeQuietly(writer);
        }

    }

}
