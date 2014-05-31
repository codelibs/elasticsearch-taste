package org.codelibs.elasticsearch.taste.river.handler;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.UUID;

import org.apache.mahout.cf.taste.eval.RecommenderBuilder;
import org.codelibs.elasticsearch.taste.TasteSystemException;
import org.codelibs.elasticsearch.taste.eval.EvaluationConfig;
import org.codelibs.elasticsearch.taste.eval.Evaluator;
import org.codelibs.elasticsearch.taste.eval.EvaluatorFactory;
import org.codelibs.elasticsearch.taste.model.ElasticsearchDataModel;
import org.codelibs.elasticsearch.taste.model.IndexInfo;
import org.codelibs.elasticsearch.taste.recommender.UserBasedRecommenderBuilder;
import org.codelibs.elasticsearch.taste.service.TasteService;
import org.codelibs.elasticsearch.taste.writer.ObjectWriter;
import org.codelibs.elasticsearch.taste.writer.ResultWriter;
import org.codelibs.elasticsearch.util.SettingsUtils;
import org.elasticsearch.client.Client;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentFactory;
import org.elasticsearch.river.RiverSettings;

public class EvalItemsFromUserHandler extends ActionHandler {
    public EvalItemsFromUserHandler(final RiverSettings settings,
            final Client client, final TasteService tasteService) {
        super(settings, client, tasteService);
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

        waitForClusterStatus(indexInfo.getUserIndex(),
                indexInfo.getItemIndex(), indexInfo.getPreferenceIndex(),
                indexInfo.getReportIndex());

        final RecommenderBuilder recommenderBuilder = new UserBasedRecommenderBuilder(
                indexInfo, rootSettings);

        final Map<String, Object> evaluatorSettings = SettingsUtils.get(
                rootSettings, "evaluator");
        final Evaluator evaluator = createEvaluator(evaluatorSettings);

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

        tasteService.evaluate(dataModel, recommenderBuilder, evaluator, writer,
                config);
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
            throw new TasteSystemException("Could not create an instance of "
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
}
