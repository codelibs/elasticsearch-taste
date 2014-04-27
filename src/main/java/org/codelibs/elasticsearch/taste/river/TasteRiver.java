package org.codelibs.elasticsearch.taste.river;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.mahout.cf.taste.impl.recommender.GenericItemBasedRecommender;
import org.apache.mahout.cf.taste.impl.recommender.GenericUserBasedRecommender;
import org.apache.mahout.cf.taste.neighborhood.UserNeighborhood;
import org.apache.mahout.cf.taste.recommender.ItemBasedRecommender;
import org.apache.mahout.cf.taste.recommender.Recommender;
import org.apache.mahout.cf.taste.similarity.ItemSimilarity;
import org.apache.mahout.cf.taste.similarity.UserSimilarity;
import org.codelibs.elasticsearch.taste.TasteConstants;
import org.codelibs.elasticsearch.taste.TasteSystemException;
import org.codelibs.elasticsearch.taste.model.ElasticsearchDataModel;
import org.codelibs.elasticsearch.taste.neighborhood.UserNeighborhoodFactory;
import org.codelibs.elasticsearch.taste.service.PrecomputeService;
import org.codelibs.elasticsearch.taste.similarity.SimilarityFactory;
import org.codelibs.elasticsearch.taste.similarity.writer.RecommendedItemsWriter;
import org.codelibs.elasticsearch.taste.similarity.writer.SimilarItemsWriter;
import org.codelibs.elasticsearch.util.SettingsUtils;
import org.codelibs.elasticsearch.util.StringUtils;
import org.elasticsearch.action.admin.cluster.health.ClusterHealthResponse;
import org.elasticsearch.action.admin.indices.mapping.delete.DeleteMappingResponse;
import org.elasticsearch.client.Client;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.river.AbstractRiverComponent;
import org.elasticsearch.river.River;
import org.elasticsearch.river.RiverName;
import org.elasticsearch.river.RiverSettings;
import org.elasticsearch.search.Scroll;

public class TasteRiver extends AbstractRiverComponent implements River {
    private static final String RECOMMEND_FROM_ITEM = "recommend_from_item";

    private static final String RECOMMEND_FROM_USER = "recommend_from_user";

    private final Client client;

    private PrecomputeService precomputeService;

    private Thread riverThread;

    @Inject
    public TasteRiver(final RiverName riverName, final RiverSettings settings,
            final Client client, final PrecomputeService precomputeService) {
        super(riverName, settings);
        this.client = client;
        this.precomputeService = precomputeService;

        logger.info("CREATE TasteRiver");
    }

    @Override
    public void start() {
        waitForClusterStatus();

        logger.info("START TasteRiver");
        try {
            final Map<String, Object> rootSettings = settings.settings();
            final Object actionObj = rootSettings.get("action");
            if (RECOMMEND_FROM_USER.equals(actionObj)) {
                final int numOfItems = SettingsUtils.get(rootSettings,
                        "num_of_items", 10);
                final int maxDuration = SettingsUtils.get(rootSettings,
                        "max_duration", 0);

                final Map<String, Object> indexInfoSettings = SettingsUtils
                        .get(rootSettings, "index_info");
                final IndexInfo indexInfo = new IndexInfo(indexInfoSettings);

                final Map<String, Object> modelInfoSettings = SettingsUtils
                        .get(rootSettings, "data_model");
                final ElasticsearchDataModel dataModel = createDataModel(
                        client, indexInfo, modelInfoSettings);

                waitForClusterStatus(indexInfo.getUserIndex(),
                        indexInfo.getItemIndex(),
                        indexInfo.getPreferenceIndex(),
                        indexInfo.getRecommendationIndex());

                final Map<String, Object> similaritySettings = SettingsUtils
                        .get(rootSettings, "similarity",
                                new HashMap<String, Object>());
                similaritySettings.put("dataModel", dataModel);
                final UserSimilarity similarity = createSimilarity(similaritySettings);

                final Map<String, Object> neighborhoodSettings = SettingsUtils
                        .get(rootSettings, "neighborhood",
                                new HashMap<String, Object>());
                neighborhoodSettings.put("dataModel", dataModel);
                neighborhoodSettings.put("userSimilarity", similarity);
                final UserNeighborhood neighborhood = createUserNeighborhood(neighborhoodSettings);

                final RecommendedItemsWriter writer = createRecommendedItemsWriter(indexInfo);

                final int degreeOfParallelism = getDegreeOfParallelism();

                final Recommender recommender = new GenericUserBasedRecommender(
                        dataModel, neighborhood, similarity);

                startRiverThread(new Runnable() {
                    @Override
                    public void run() {
                        try {
                            precomputeService.compute(recommender, writer,
                                    numOfItems, degreeOfParallelism,
                                    maxDuration);
                        } catch (final Exception e) {
                            logger.error("River {} is failed.", e,
                                    riverName.name());
                        } finally {
                            deleteRiver();
                        }
                    }
                }, "River" + riverName.name());
            } else if (RECOMMEND_FROM_ITEM.equals(actionObj)) {
                final int numOfItems = SettingsUtils.get(rootSettings,
                        "num_of_items", 10);
                final int maxDuration = SettingsUtils.get(rootSettings,
                        "max_duration", 0);

                final Map<String, Object> indexInfoSettings = SettingsUtils
                        .get(rootSettings, "index_info");
                final IndexInfo indexInfo = new IndexInfo(indexInfoSettings);

                final Map<String, Object> modelInfoSettings = SettingsUtils
                        .get(rootSettings, "data_model");
                final ElasticsearchDataModel dataModel = createDataModel(
                        client, indexInfo, modelInfoSettings);

                waitForClusterStatus(indexInfo.getUserIndex(),
                        indexInfo.getItemIndex(),
                        indexInfo.getPreferenceIndex(),
                        indexInfo.getItemSimilarityIndex());

                final Map<String, Object> similaritySettings = SettingsUtils
                        .get(rootSettings, "similarity",
                                new HashMap<String, Object>());
                similaritySettings.put("dataModel", dataModel);
                final ItemSimilarity similarity = createSimilarity(similaritySettings);

                final ItemBasedRecommender recommender = new GenericItemBasedRecommender(
                        dataModel, similarity);

                final SimilarItemsWriter writer = createSimilarItemsWriter(indexInfo);

                final int degreeOfParallelism = getDegreeOfParallelism();

                startRiverThread(new Runnable() {
                    @Override
                    public void run() {
                        try {
                            precomputeService.compute(recommender, writer,
                                    numOfItems, degreeOfParallelism,
                                    maxDuration);
                        } catch (final Exception e) {
                            logger.error("River {} is failed.", e,
                                    riverName.name());
                        } finally {
                            deleteRiver();
                        }
                    }
                }, "River" + riverName.name());
            } else {
                logger.info("River {} has no actions. Deleting...",
                        riverName.name());
            }
        } finally {
            if (riverThread == null) {
                deleteRiver();
            }
        }
    }

    protected void waitForClusterStatus(final String... indices) {
        final ClusterHealthResponse response = client.admin().cluster()
                .prepareHealth(indices).setWaitForYellowStatus().execute()
                .actionGet();
        final List<String> failures = response.getAllValidationFailures();
        if (!failures.isEmpty()) {
            throw new TasteSystemException("Cluster is not available: "
                    + failures.toString());
        }
    }

    protected void startRiverThread(final Runnable runnable, final String name) {
        riverThread = new Thread(runnable, name);
        try {
            riverThread.start();
        } catch (final Exception e) {
            logger.error("Failed to start {}.", e, name);
            riverThread = null;
        }
    }

    private int getDegreeOfParallelism() {
        int degreeOfParallelism = Runtime.getRuntime().availableProcessors() - 1;
        if (degreeOfParallelism < 1) {
            degreeOfParallelism = 1;
        }
        return degreeOfParallelism;
    }

    protected RecommendedItemsWriter createRecommendedItemsWriter(
            final IndexInfo indexInfo) {
        final RecommendedItemsWriter writer = new RecommendedItemsWriter(
                client, indexInfo.getRecommendationIndex());
        writer.setType(indexInfo.getRecommendationType());
        writer.setItemIdField(indexInfo.getItemIdField());
        writer.setItemsField(indexInfo.getItemsField());
        writer.setUserIdField(indexInfo.getUserIdField());
        writer.setValueField(indexInfo.getValueField());
        writer.setTimestampField(indexInfo.getTimestampField());

        writer.open();

        return writer;
    }

    protected SimilarItemsWriter createSimilarItemsWriter(
            final IndexInfo indexInfo) {
        final SimilarItemsWriter writer = new SimilarItemsWriter(client,
                indexInfo.getItemSimilarityIndex());
        writer.setType(indexInfo.getItemSimilarityType());
        writer.setItemIdField(indexInfo.getItemIdField());
        writer.setItemsField(indexInfo.getItemsField());
        writer.setValueField(indexInfo.getValueField());
        writer.setTimestampField(indexInfo.getTimestampField());

        writer.open();

        return writer;
    }

    protected UserNeighborhood createUserNeighborhood(
            final Map<String, Object> neighborhoodSettings) {
        final String factoryName = SettingsUtils
                .get(neighborhoodSettings, "factory",
                        "org.codelibs.elasticsearch.taste.neighborhood.NearestNUserNeighborhoodFactory");
        try {
            final Class<?> clazz = Class.forName(factoryName);
            final UserNeighborhoodFactory userNeighborhoodFactory = (UserNeighborhoodFactory) clazz
                    .newInstance();
            userNeighborhoodFactory.init(neighborhoodSettings);
            return userNeighborhoodFactory.create();
        } catch (ClassNotFoundException | InstantiationException
                | IllegalAccessException e) {
            throw new TasteSystemException("Could not create an instance of "
                    + factoryName, e);
        }
    }

    protected <T> T createSimilarity(
            final Map<String, Object> similaritySettings) {
        final String factoryName = SettingsUtils
                .get(similaritySettings, "factory",
                        "org.codelibs.elasticsearch.taste.similarity.LogLikelihoodSimilarityFactory");
        try {
            final Class<?> clazz = Class.forName(factoryName);
            @SuppressWarnings("unchecked")
            final SimilarityFactory<T> similarityFactory = (SimilarityFactory<T>) clazz
                    .newInstance();
            similarityFactory.init(similaritySettings);
            return similarityFactory.create();
        } catch (ClassNotFoundException | InstantiationException
                | IllegalAccessException e) {
            throw new TasteSystemException("Could not create an instance of "
                    + factoryName, e);
        }
    }

    @Override
    public void close() {
        logger.info("CLOSE TasteRiver");
    }

    protected void deleteRiver() {
        final DeleteMappingResponse deleteMappingResponse = client.admin()
                .indices().prepareDeleteMapping("_river")
                .setType(riverName.name()).execute().actionGet();
        if (deleteMappingResponse.isAcknowledged()) {
            logger.info("Deleted " + riverName.name() + "river.");
        } else {
            logger.warn("Failed to delete " + riverName.name() + ". Response: "
                    + deleteMappingResponse.toString());
        }
    }

    protected ElasticsearchDataModel createDataModel(final Client client,
            final IndexInfo indexInfo,
            final Map<String, Object> modelInfoSettings) {
        if (StringUtils.isBlank(indexInfo.getUserIndex())) {
            throw new TasteSystemException("User Index is blank.");
        }
        if (StringUtils.isBlank(indexInfo.getPreferenceIndex())) {
            throw new TasteSystemException("Preference Index is blank.");
        }
        if (StringUtils.isBlank(indexInfo.getItemIndex())) {
            throw new TasteSystemException("Item Index is blank.");
        }

        final String className = SettingsUtils.get(modelInfoSettings, "class");
        if (StringUtils.isBlank(className)) {
            throw new TasteSystemException("A class name is blank.");
        }

        try {
            final Class<?> clazz = Class.forName(className);
            final ElasticsearchDataModel model = (ElasticsearchDataModel) clazz
                    .newInstance();
            model.setClient(client);
            model.setPreferenceIndex(indexInfo.getPreferenceIndex());
            model.setPreferenceType(indexInfo.getPreferenceType());
            model.setUserIndex(indexInfo.getUserIndex());
            model.setUserType(indexInfo.getUserType());
            model.setItemIndex(indexInfo.getItemIndex());
            model.setItemType(indexInfo.getItemType());
            model.setUserIdField(indexInfo.getUserIdField());
            model.setItemIdField(indexInfo.getItemIdField());
            model.setValueField(indexInfo.getValueField());
            model.setTimestampField(indexInfo.getTimestampField());

            final Map<String, Object> scrollSettings = SettingsUtils.get(
                    modelInfoSettings, "scroll");
            model.setScrollSize(SettingsUtils.get(scrollSettings, "size", 1000));
            model.setScrollKeepAlive(new Scroll(TimeValue
                    .timeValueSeconds(SettingsUtils.get(scrollSettings,
                            "keep_alive", 60))));

            final Map<String, Object> querySettings = SettingsUtils.get(
                    modelInfoSettings, "query");
            final String userQuery = SettingsUtils.get(querySettings, "user");
            if (StringUtils.isNotBlank(userQuery)) {
                model.setUserQueryBuilder(QueryBuilders.queryString(userQuery));
            }
            final String itemQuery = SettingsUtils.get(querySettings, "item");
            if (StringUtils.isNotBlank(itemQuery)) {
                model.setUserQueryBuilder(QueryBuilders.queryString(itemQuery));
            }

            final Map<String, Object> cacheSettings = SettingsUtils.get(
                    modelInfoSettings, "cache");
            final Object weight = SettingsUtils.get(cacheSettings, "weight");
            if (weight instanceof Number) {
                model.setMaxCacheWeight(((Integer) weight).longValue());
            } else {
                final long weightSize = parseWeight(weight.toString());
                if (weightSize > 0) {
                    model.setMaxCacheWeight(weightSize);
                }
            }
            return model;
        } catch (ClassNotFoundException | InstantiationException
                | IllegalAccessException e) {
            throw new TasteSystemException("Could not create an instance of "
                    + className);
        }
    }

    private long parseWeight(final String value) {
        if (StringUtils.isBlank(value)) {
            return 0;
        }
        try {
            final char lastChar = value.charAt(value.length() - 1);
            if (lastChar == 'g' || lastChar == 'G') {
                return Long.parseLong(value.substring(0, value.length() - 2));
            } else if (lastChar == 'm' || lastChar == 'M') {
                return Long.parseLong(value.substring(0, value.length() - 2));
            } else if (lastChar == 'k' || lastChar == 'K') {
                return Long.parseLong(value.substring(0, value.length() - 2));
            }
            return Long.parseLong(value);
        } catch (final Exception e) {
            logger.warn("Failed to parse a weight: {}", e, value);
        }
        return 0;
    }

    protected static class IndexInfo {
        private String preferenceIndex;

        private String preferenceType;

        private String userIndex;

        private String userType;

        private String itemIndex;

        private String itemType;

        private String recommendationIndex;

        private String recommendationType;

        private String itemSimilarityIndex;

        private String itemSimilarityType;

        private String userIdField;

        private String itemIdField;

        private String valueField;

        private String timestampField;

        private String itemsField;

        protected IndexInfo(final Map<String, Object> indexInfoSettings) {
            final String defaultIndex = SettingsUtils.get(indexInfoSettings,
                    "index");

            final Map<String, Object> preferenceSettings = SettingsUtils.get(
                    indexInfoSettings, "preference");
            preferenceIndex = SettingsUtils.get(preferenceSettings, "index",
                    defaultIndex);
            preferenceType = SettingsUtils.get(preferenceSettings, "type",
                    TasteConstants.PREFERENCE_TYPE);

            final Map<String, Object> userSettings = SettingsUtils.get(
                    indexInfoSettings, "user");
            userIndex = SettingsUtils.get(userSettings, "index", defaultIndex);
            userType = SettingsUtils.get(userSettings, "type",
                    TasteConstants.USER_TYPE);

            final Map<String, Object> itemSettings = SettingsUtils.get(
                    indexInfoSettings, "item");
            itemIndex = SettingsUtils.get(itemSettings, "index", defaultIndex);
            itemType = SettingsUtils.get(itemSettings, "type",
                    TasteConstants.ITEM_TYPE);

            final Map<String, Object> recommendationSettings = SettingsUtils
                    .get(indexInfoSettings, "recommendation");
            recommendationIndex = SettingsUtils.get(recommendationSettings,
                    "index", defaultIndex);
            recommendationType = SettingsUtils.get(recommendationSettings,
                    "type", TasteConstants.RECOMMENDATION_TYPE);

            final Map<String, Object> itemSimilaritySettings = SettingsUtils
                    .get(indexInfoSettings, "item_similarity");
            itemSimilarityIndex = SettingsUtils.get(itemSimilaritySettings,
                    "index", defaultIndex);
            itemSimilarityType = SettingsUtils.get(itemSimilaritySettings,
                    "type", TasteConstants.ITEM_SIMILARITY_TYPE);

            final Map<String, Object> fieldSettings = SettingsUtils.get(
                    indexInfoSettings, "field");
            userIdField = SettingsUtils.get(fieldSettings, "user_id",
                    TasteConstants.USER_ID_FIELD);
            itemIdField = SettingsUtils.get(fieldSettings, "item_id",
                    TasteConstants.ITEM_ID_FIELD);
            valueField = SettingsUtils.get(fieldSettings, "value",
                    TasteConstants.VALUE_FIELD);
            timestampField = SettingsUtils.get(fieldSettings, "timestamp",
                    TasteConstants.TIMESTAMP_FIELD);
            itemsField = SettingsUtils.get(fieldSettings, "items",
                    TasteConstants.ITEMS_FILED);
        }

        public String getPreferenceIndex() {
            return preferenceIndex;
        }

        public String getPreferenceType() {
            return preferenceType;
        }

        public String getUserIndex() {
            return userIndex;
        }

        public String getUserType() {
            return userType;
        }

        public String getItemIndex() {
            return itemIndex;
        }

        public String getItemType() {
            return itemType;
        }

        public String getRecommendationIndex() {
            return recommendationIndex;
        }

        public String getRecommendationType() {
            return recommendationType;
        }

        public String getItemSimilarityIndex() {
            return itemSimilarityIndex;
        }

        public String getItemSimilarityType() {
            return itemSimilarityType;
        }

        public String getUserIdField() {
            return userIdField;
        }

        public String getItemIdField() {
            return itemIdField;
        }

        public String getValueField() {
            return valueField;
        }

        public String getTimestampField() {
            return timestampField;
        }

        public String getItemsField() {
            return itemsField;
        }
    }
}
