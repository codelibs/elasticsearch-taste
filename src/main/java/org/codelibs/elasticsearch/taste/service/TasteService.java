package org.codelibs.elasticsearch.taste.service;

import java.util.HashMap;
import java.util.Map;
import java.util.UUID;
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
import org.apache.mahout.common.RandomUtils;
import org.codelibs.elasticsearch.taste.eval.Evaluation;
import org.codelibs.elasticsearch.taste.eval.EvaluationConfig;
import org.codelibs.elasticsearch.taste.eval.Evaluator;
import org.codelibs.elasticsearch.taste.recommender.UserBasedRecommenderBuilder;
import org.codelibs.elasticsearch.taste.worker.RecommendedItemsWorker;
import org.codelibs.elasticsearch.taste.worker.SimilarItemsWorker;
import org.codelibs.elasticsearch.taste.writer.ItemWriter;
import org.codelibs.elasticsearch.taste.writer.ObjectWriter;
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
            final ItemWriter writer, final int numOfItems,
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
            final UserBasedRecommender recommender, final ItemWriter writer,
            final int numOfRecommendedItems, final int degreeOfParallelism,
            final int maxDuration) {
        final ExecutorService executorService = Executors
                .newFixedThreadPool(degreeOfParallelism + 1);
        try {

            logger.info("Recommender: {}", recommender);
            logger.info("NumOfRecommendedItems: {}", numOfRecommendedItems);
            logger.info("MaxDuration: {}", maxDuration);

            final LongPrimitiveIterator userIDs = dataModel.getUserIDs();

            for (int n = 0; n < degreeOfParallelism; n++) {
                executorService.execute(new RecommendedItemsWorker(n,
                        recommender, userIDs, numOfRecommendedItems, writer));
            }

            executorService.shutdown();
            boolean succeeded = false;
            try {
                succeeded = awaitExecutorTermination(executorService,
                        maxDuration);
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
            final ItemBasedRecommender recommender, final ItemWriter writer,
            final int numOfMostSimilarItems, final int degreeOfParallelism,
            final int maxDuration) {
        logger.info("Recommender: {}", recommender.toString());
        logger.info("NumOfMostSimilarItems: {}", numOfMostSimilarItems);
        logger.info("MaxDuration: {}", maxDuration);

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
                succeeded = awaitExecutorTermination(executorService,
                        maxDuration);
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

    public void evaluate(final DataModel dataModel,
            final RecommenderBuilder recommenderBuilder,
            final Evaluator evaluator, final ObjectWriter writer,
            final EvaluationConfig config) {

        final String evaluatorId = UUID.randomUUID().toString()
                .replace("-", "");
        evaluator.setId(evaluatorId);

        RandomUtils.useTestSeed();
        try {
            long time = System.currentTimeMillis();
            final Evaluation evaluation = evaluator.evaluate(
                    recommenderBuilder, dataModel, config);
            time = System.currentTimeMillis() - time;

            String reportType;
            if (recommenderBuilder instanceof UserBasedRecommenderBuilder) {
                reportType = "user_based";
            } else {
                reportType = "unknown";
            }

            final Map<String, Object> rootObj = new HashMap<>();
            rootObj.put("report_type", reportType);
            rootObj.put("evaluator_id", evaluator);
            final Map<String, Object> evaluationObj = new HashMap<>();

            final Map<String, Object> timeObj = new HashMap<>();
            timeObj.put("average_processing",
                    evaluation.getAverageProcessingTime());
            timeObj.put("max_processing", evaluation.getMaxProcessingTime());
            timeObj.put("total_processing", evaluation.getTotalProcessingTime());
            evaluationObj.put("time", timeObj);

            final Map<String, Object> preferenceObj = new HashMap<>();
            preferenceObj.put("success", evaluation.getSuccessful());
            preferenceObj.put("failure", evaluation.getFailure());
            preferenceObj.put("no_estimate", evaluation.getNoEstimate());
            preferenceObj.put("total", evaluation.getTotalPreference());
            evaluationObj.put("preference", preferenceObj);

            final Map<String, Object> targetObj = new HashMap<>();
            targetObj.put("training", evaluation.getTraining());
            targetObj.put("test", evaluation.getTest());
            evaluationObj.put("target", targetObj);

            evaluationObj.put("score", evaluation.getScore());
            rootObj.put("evaluation", evaluationObj);
            final Map<String, Object> configObj = new HashMap<>();
            configObj
                    .put("training_percentage", config.getTrainingPercentage());
            configObj.put("evaluation_percentage",
                    config.getEvaluationPercentage());
            configObj.put("margin_for_error", config.getMarginForError());
            rootObj.put("config", configObj);

            writer.write(rootObj);
        } catch (final TasteException e) {
            logger.error("Evaluator {}({}) is failed.", e, evaluator, config);
        } finally {
            IOUtils.closeQuietly(writer);
        }
    }

    protected boolean awaitExecutorTermination(
            final ExecutorService executorService, final int maxDuration)
            throws InterruptedException {
        if (maxDuration == 0) {
            return executorService.awaitTermination(Long.MAX_VALUE,
                    TimeUnit.NANOSECONDS);
        }
        return executorService.awaitTermination(maxDuration, TimeUnit.MINUTES);
    }
}
