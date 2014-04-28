package org.codelibs.elasticsearch.taste.service;

import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

import org.apache.mahout.cf.taste.common.TasteException;
import org.apache.mahout.cf.taste.eval.RecommenderBuilder;
import org.apache.mahout.cf.taste.impl.common.LongPrimitiveIterator;
import org.apache.mahout.cf.taste.model.DataModel;
import org.apache.mahout.cf.taste.recommender.ItemBasedRecommender;
import org.apache.mahout.cf.taste.recommender.Recommender;
import org.apache.mahout.cf.taste.recommender.UserBasedRecommender;
import org.codelibs.elasticsearch.taste.similarity.worker.RecommendedItemsWorker;
import org.codelibs.elasticsearch.taste.similarity.worker.SimilarItemsWorker;
import org.codelibs.elasticsearch.taste.similarity.writer.ItemsWriter;
import org.codelibs.elasticsearch.util.IOUtils;
import org.elasticsearch.ElasticsearchException;
import org.elasticsearch.common.component.AbstractLifecycleComponent;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.settings.Settings;

public class TasteService extends AbstractLifecycleComponent<TasteService> {

    @Inject
    public TasteService(final Settings settings) {
        super(settings);
        logger.info("CREATE TasteService");
    }

    @Override
    protected void doStart() throws ElasticsearchException {
        logger.info("START TasteService");
    }

    @Override
    protected void doStop() throws ElasticsearchException {
        logger.info("STOP TasteService");
    }

    @Override
    protected void doClose() throws ElasticsearchException {
        logger.info("CLOSE TasteService");
    }

    public void compute(final DataModel dataModel,
            final RecommenderBuilder recommenderBuilder,
            final ItemsWriter writer, final int numOfItems,
            final int degreeOfParallelism, final int maxDuration) {
        try {
            final Recommender recommender = recommenderBuilder
                    .buildRecommender(dataModel);
            if (recommender instanceof UserBasedRecommender) {
                compute(dataModel, (UserBasedRecommender) recommender, writer,
                        numOfItems, degreeOfParallelism, maxDuration);
            } else if (recommender instanceof ItemBasedRecommender) {
                compute(dataModel, (ItemBasedRecommender) recommender, writer,
                        numOfItems, degreeOfParallelism, maxDuration);
            } else {
                logger.error("Unknown a recommender: {}", recommender);
            }
        } catch (final TasteException e) {
            logger.error("Failed to build a recommender from: {}", e,
                    recommenderBuilder);
        }
    }

    protected void compute(final DataModel dataModel,
            final UserBasedRecommender recommender, final ItemsWriter writer,
            final int numOfRecommendedItems, final int degreeOfParallelism,
            final int maxDuration) {
        final ExecutorService executorService = Executors
                .newFixedThreadPool(degreeOfParallelism + 1);
        try {

            logger.info("Recommender: ", recommender);
            logger.info("NumOfRecommendedItems: ", numOfRecommendedItems);
            logger.info("MaxDuration: ", maxDuration);

            final LongPrimitiveIterator userIDs = dataModel.getUserIDs();

            for (int n = 0; n < degreeOfParallelism; n++) {
                executorService.execute(new RecommendedItemsWorker(n,
                        recommender, userIDs, numOfRecommendedItems, writer));
            }

            executorService.shutdown();
            boolean succeeded = false;
            try {
                succeeded = executorService.awaitTermination(maxDuration,
                        TimeUnit.MINUTES);
                if (!succeeded) {
                    logger.warn(
                            "Unable to complete the computation in {} minutes!",
                            maxDuration);
                }
            } catch (final InterruptedException e) {
                logger.warn("Interrupted a executor.", e);
            }
            if (!succeeded) {
                executorService.shutdownNow();
            }
        } catch (final TasteException e) {
            logger.error("Recommender {} is failed.", e, recommender);
        } finally {
            IOUtils.closeQuietly(writer);
        }

    }

    protected void compute(final DataModel dataModel,
            final ItemBasedRecommender recommender, final ItemsWriter writer,
            final int numOfMostSimilarItems, final int degreeOfParallelism,
            final int maxDuration) {
        logger.info("Recommender: ", recommender.toString());
        logger.info("NumOfMostSimilarItems: ", numOfMostSimilarItems);
        logger.info("MaxDuration: ", maxDuration);

        final ExecutorService executorService = Executors
                .newFixedThreadPool(degreeOfParallelism + 1);
        try {
            final LongPrimitiveIterator itemIDs = dataModel.getItemIDs();

            for (int n = 0; n < degreeOfParallelism; n++) {
                executorService.execute(new SimilarItemsWorker(n, recommender,
                        itemIDs, numOfMostSimilarItems, writer));
            }

            executorService.shutdown();
            boolean succeeded = false;
            try {
                succeeded = executorService.awaitTermination(maxDuration,
                        TimeUnit.MINUTES);
                if (!succeeded) {
                    logger.warn(
                            "Unable to complete the computation in {} minutes!",
                            maxDuration);
                }
            } catch (final InterruptedException e) {
                logger.warn("Interrupted a executor.", e);
            }
            if (!succeeded) {
                executorService.shutdownNow();
            }
        } catch (final TasteException e) {
            logger.error("Recommender {} is failed.", e, recommender);
        } finally {
            IOUtils.closeQuietly(writer);
        }

    }

}
