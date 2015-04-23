package org.codelibs.elasticsearch.taste.rest.handler;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.UUID;

import org.apache.mahout.common.RandomUtils;
import org.codelibs.elasticsearch.taste.eval.Evaluation;
import org.codelibs.elasticsearch.taste.eval.EvaluationConfig;
import org.codelibs.elasticsearch.taste.eval.Evaluator;
import org.codelibs.elasticsearch.taste.eval.EvaluatorFactory;
import org.codelibs.elasticsearch.taste.eval.RecommenderBuilder;
import org.codelibs.elasticsearch.taste.exception.TasteException;
import org.codelibs.elasticsearch.taste.model.DataModel;
import org.codelibs.elasticsearch.taste.model.ElasticsearchDataModel;
import org.codelibs.elasticsearch.taste.model.IndexInfo;
import org.codelibs.elasticsearch.taste.recommender.UserBasedRecommenderBuilder;
import org.codelibs.elasticsearch.taste.service.TasteService;
import org.codelibs.elasticsearch.taste.writer.ObjectWriter;
import org.codelibs.elasticsearch.taste.writer.ResultWriter;
import org.codelibs.elasticsearch.util.admin.ClusterUtils;
import org.codelibs.elasticsearch.util.io.IOUtils;
import org.codelibs.elasticsearch.util.settings.SettingsUtils;
import org.elasticsearch.client.Client;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentFactory;

public class EvalItemsFromUserHandler extends RecommendationHandler {
    private Evaluator evaluator;

    public EvalItemsFromUserHandler(final Settings settings,
            final Map<String, Object> sourceMap, final Client client,
            final TasteService tasteService) {
        super(settings, sourceMap, client, tasteService);
    }

    @Override
    public void execute() {
        final double trainingPercentage = SettingsUtils.get(rootSettings,
                "training_percentage", 1.0);
        final double evaluationPercentage = SettingsUtils.get(rootSettings,
                "evaluation_percentage", 1.0);
        final double marginForError = SettingsUtils.get(rootSettings,
                "margin_for_error", 0.5);
        final EvaluationConfig config = new EvaluationConfig();
        config.setTrainingPercentage(trainingPercentage);
        config.setEvaluationPercentage(evaluationPercentage);
        config.setMarginForError((float) marginForError);

        final Map<String, Object> indexInfoSettings = SettingsUtils.get(
                rootSettings, "index_info");
        final IndexInfo indexInfo = new IndexInfo(indexInfoSettings);

        final Map<String, Object> modelInfoSettings = SettingsUtils.get(
                rootSettings, "data_model");
        final ElasticsearchDataModel dataModel = createDataModel(client,
                indexInfo, modelInfoSettings);

        ClusterUtils.waitForAvailable(client, indexInfo.getUserIndex(),
                indexInfo.getItemIndex(), indexInfo.getPreferenceIndex(),
                indexInfo.getReportIndex());

        final RecommenderBuilder recommenderBuilder = new UserBasedRecommenderBuilder(
                indexInfo, rootSettings);

        final Map<String, Object> evaluatorSettings = SettingsUtils.get(
                rootSettings, "evaluator");
        evaluator = createEvaluator(evaluatorSettings);

        final ObjectWriter writer = createReportWriter(indexInfo);

        final Map<String, Object> resultSettings = SettingsUtils.get(
                rootSettings, "result", new HashMap<String, Object>());
        final Boolean writerEnabled = SettingsUtils.get(resultSettings,
                "enabled", false);
        if (writerEnabled.booleanValue()) {
            final int maxQueueSize = SettingsUtils.get(resultSettings,
                    "queue_size", 1000);
            final ResultWriter resultWriter = createResultWriter(indexInfo,
                    maxQueueSize);
            if (resultWriter != null) {
                evaluator.setResultWriter(resultWriter);
            }
        }

        evaluate(dataModel, recommenderBuilder, evaluator, writer, config);
    }

    protected void evaluate(final DataModel dataModel,
            final RecommenderBuilder recommenderBuilder,
            final Evaluator evaluator, final ObjectWriter writer,
            final EvaluationConfig config) {

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
            rootObj.put("evaluator_id", evaluator.getId());
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

    protected ResultWriter createResultWriter(final IndexInfo indexInfo,
            final int maxQueueSize) {
        final ResultWriter writer = new ResultWriter(client,
                indexInfo.getResultIndex(), indexInfo.getResultType());
        writer.setUserIdField(indexInfo.getUserIdField());
        writer.setItemIdField(indexInfo.getItemIdField());
        writer.setMaxQueueSize(maxQueueSize);
        writer.setTimestampField(indexInfo.getTimestampField());
        try {
            final XContentBuilder builder = XContentFactory.jsonBuilder()//
                    .startObject()//
                    .startObject(indexInfo.getResultType())//
                    .startObject("properties")//

                    // @timestamp
                    .startObject(indexInfo.getTimestampField())//
                    .field("type", "date")//
                    .field("format", "dateOptionalTime")//
                    .endObject()//

                    // result_type
                    .startObject("result_type")//
                    .field("type", "string")//
                    .field("index", "not_analyzed")//
                    .endObject()//

                    // evaluator_id
                    .startObject("evaluator_id")//
                    .field("type", "string")//
                    .field("index", "not_analyzed")//
                    .endObject()//

                    // user_id
                    .startObject(indexInfo.getItemIdField())//
                    .field("type", "long")//
                    .endObject()//

                    // item_id
                    .startObject(indexInfo.getItemIdField())//
                    .field("type", "long")//
                    .endObject()//

                    // actual
                    .startObject("actual")//
                    .field("type", "float")//
                    .endObject()//

                    // estimate
                    .startObject("estimate")//
                    .field("type", "float")//
                    .endObject()//

                    // computing_time
                    .startObject("computing_time")//
                    .field("type", "long")//
                    .endObject()//

                    .endObject()//
                    .endObject()//
                    .endObject();
            writer.setMapping(builder);
        } catch (final IOException e) {
            logger.info("Failed to create a mapping {}/{}.", e,
                    indexInfo.getReportIndex(), indexInfo.getReportType());
        }

        writer.open();

        return writer;
    }

    protected Evaluator createEvaluator(
            final Map<String, Object> evaluatorSettings) {
        final String factoryName = SettingsUtils.get(evaluatorSettings,
                "factory",
                "org.codelibs.elasticsearch.taste.eval.RMSEvaluatorFactory");
        try {
            final Class<?> clazz = Class.forName(factoryName);
            final EvaluatorFactory recommenderEvaluatorFactory = (EvaluatorFactory) clazz
                    .newInstance();
            recommenderEvaluatorFactory.init(evaluatorSettings);
            final Evaluator evaluator = recommenderEvaluatorFactory.create();
            final String evaluatorId = SettingsUtils.get(evaluatorSettings,
                    "id", UUID.randomUUID().toString().replace("-", ""));
            evaluator.setId(evaluatorId);
            return evaluator;
        } catch (ClassNotFoundException | InstantiationException
                | IllegalAccessException e) {
            throw new TasteException("Could not create an instance of "
                    + factoryName, e);
        }
    }

    protected ObjectWriter createReportWriter(final IndexInfo indexInfo) {
        final ObjectWriter writer = new ObjectWriter(client,
                indexInfo.getReportIndex(), indexInfo.getReportType());
        writer.setTimestampField(indexInfo.getTimestampField());
        try {
            final XContentBuilder builder = XContentFactory.jsonBuilder()//
                    .startObject()//
                    .startObject(indexInfo.getReportType())//
                    .startObject("properties")//

                    // @timestamp
                    .startObject(indexInfo.getTimestampField())//
                    .field("type", "date")//
                    .field("format", "dateOptionalTime")//
                    .endObject()//

                    // report_type
                    .startObject("report_type")//
                    .field("type", "string")//
                    .field("index", "not_analyzed")//
                    .endObject()//

                    // evaluator_id
                    .startObject("evaluator_id")//
                    .field("type", "string")//
                    .field("index", "not_analyzed")//
                    .endObject()//

                    .endObject()//
                    .endObject()//
                    .endObject();
            writer.setMapping(builder);
        } catch (final IOException e) {
            logger.info("Failed to create a mapping {}/{}.", e,
                    indexInfo.getReportIndex(), indexInfo.getReportType());
        }

        writer.open();

        return writer;
    }

    @Override
    public void close() {
        evaluator.interrupt();
    }
}
