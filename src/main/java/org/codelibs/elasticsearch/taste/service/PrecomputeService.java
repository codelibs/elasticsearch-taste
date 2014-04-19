package org.codelibs.elasticsearch.taste.service;

import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

import org.apache.commons.io.IOUtils;
import org.apache.mahout.cf.taste.common.TasteException;
import org.apache.mahout.cf.taste.impl.common.LongPrimitiveIterator;
import org.apache.mahout.cf.taste.model.DataModel;
import org.apache.mahout.cf.taste.recommender.ItemBasedRecommender;
import org.apache.mahout.cf.taste.recommender.UserBasedRecommender;
import org.codelibs.elasticsearch.taste.similarity.precompute.RecommendedItemsWriter;
import org.codelibs.elasticsearch.taste.similarity.precompute.SimilarItemsWriter;
import org.codelibs.elasticsearch.taste.similarity.worker.RecommendedItemsWorker;
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
    }

    @Override
    protected void doStart() throws ElasticsearchException {
        logger.info("START PrecomputeService");
    }

    @Override
    protected void doStop() throws ElasticsearchException {
        logger.info("STOP PrecomputeService");
    }

    @Override
    protected void doClose() throws ElasticsearchException {
        logger.info("CLOSE PrecomputeService");
    }

    public void compute(final UserBasedRecommender recommender,
            final RecommendedItemsWriter writer,
            final int numOfRecommendedItems, final int degreeOfParallelism,
            final int maxDurationInHours) {

        final ExecutorService executorService = Executors
                .newFixedThreadPool(degreeOfParallelism + 1);
        try {

            final DataModel dataModel = recommender.getDataModel();
            final LongPrimitiveIterator userIDs = dataModel.getUserIDs();

            for (int n = 0; n < degreeOfParallelism; n++) {
                executorService.execute(new RecommendedItemsWorker(n,
                        recommender, userIDs, numOfRecommendedItems, writer));
            }
        } catch (final TasteException e) {
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

    public void compute(final ItemBasedRecommender recommender,
            final SimilarItemsWriter writer, final int numOfMostSimilarItems,
            final int degreeOfParallelism, final int maxDurationInHours) {

        final ExecutorService executorService = Executors
                .newFixedThreadPool(degreeOfParallelism + 1);
        try {
            final DataModel dataModel = recommender.getDataModel();
            final LongPrimitiveIterator itemIDs = dataModel.getItemIDs();

            for (int n = 0; n < degreeOfParallelism; n++) {
                executorService.execute(new SimilarItemsWorker(n, recommender,
                        itemIDs, numOfMostSimilarItems, writer));
            }
        } catch (final TasteException e) {
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
