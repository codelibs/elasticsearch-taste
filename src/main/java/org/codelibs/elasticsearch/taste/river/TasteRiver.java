package org.codelibs.elasticsearch.taste.river;

import java.util.HashMap;
import java.util.Map;

import org.apache.mahout.cf.taste.impl.recommender.GenericUserBasedRecommender;
import org.apache.mahout.cf.taste.neighborhood.UserNeighborhood;
import org.apache.mahout.cf.taste.similarity.UserSimilarity;
import org.codelibs.elasticsearch.taste.TasteConstants;
import org.codelibs.elasticsearch.taste.TasteSystemException;
import org.codelibs.elasticsearch.taste.model.ElasticsearchDataModel;
import org.codelibs.elasticsearch.taste.neighborhood.UserNeighborhoodFactory;
import org.codelibs.elasticsearch.taste.service.PrecomputeService;
import org.codelibs.elasticsearch.taste.similarity.UserSimilarityFactory;
import org.codelibs.elasticsearch.util.SettingsUtils;
import org.codelibs.elasticsearch.util.StringUtils;
import org.elasticsearch.action.admin.indices.mapping.delete.DeleteMappingResponse;
import org.elasticsearch.client.Client;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.river.AbstractRiverComponent;
import org.elasticsearch.river.River;
import org.elasticsearch.river.RiverName;
import org.elasticsearch.river.RiverSettings;
import org.elasticsearch.search.Scroll;

public class TasteRiver extends AbstractRiverComponent implements River {
    private final Client client;

    @Inject
    public TasteRiver(final RiverName riverName, final RiverSettings settings,
            final Client client, final PrecomputeService precomputeService) {
        super(riverName, settings);
        this.client = client;
        logger.info("CREATE TasteRiver");
    }

    @Override
    public void start() {
        logger.info("START TasteRiver");

        final Map<String, Object> rootSettings = settings.settings();
        final Object actionObj = rootSettings.get("action");
        if ("user".equals(actionObj)) {
            SettingsUtils.get(rootSettings, "num_of_items", 10);
            final IndexInfo indexInfo = new IndexInfo(
                    (Map<String, Object>) SettingsUtils.get(rootSettings,
                            "index_info"));
            final ElasticsearchDataModel dataModel = createDataModel(client,
                    indexInfo, (Map<String, Object>) SettingsUtils.get(
                            rootSettings, "data_model"));

            final Map<String, Object> similaritySettings = SettingsUtils.get(
                    rootSettings, "similarity", new HashMap<String, Object>());
            similaritySettings.put("similaritySettings", dataModel);
            final UserSimilarity similarity = createUserSimilarity(similaritySettings);

            final Map<String, Object> neighborhoodSettings = SettingsUtils
                    .get(rootSettings, "neighborhood",
                            new HashMap<String, Object>());
            neighborhoodSettings.put("dataModel", dataModel);
            neighborhoodSettings.put("userSimilarity", similarity);
            final UserNeighborhood neighborhood = createUserNeighborhood(neighborhoodSettings);

            new GenericUserBasedRecommender(dataModel, neighborhood, similarity);

        } else if ("item".equals(actionObj)) {

        } else {
            logger.info("River {} has no actions. Deleting...",
                    riverName.name());
            delete();
        }

    }

    private UserNeighborhood createUserNeighborhood(
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

    private UserSimilarity createUserSimilarity(
            final Map<String, Object> similaritySettings) {
        final String factoryName = SettingsUtils
                .get(similaritySettings, "factory",
                        "org.codelibs.elasticsearch.taste.similarity.LogLikelihoodSimilarityFactory");
        try {
            final Class<?> clazz = Class.forName(factoryName);
            final UserSimilarityFactory userSimilarityFactory = (UserSimilarityFactory) clazz
                    .newInstance();
            userSimilarityFactory.init(similaritySettings);
            return userSimilarityFactory.create();
        } catch (ClassNotFoundException | InstantiationException
                | IllegalAccessException e) {
            throw new TasteSystemException("Could not create an instance of "
                    + factoryName, e);
        }
    }

    @Override
    public void close() {
        logger.info("CLOSE TasteRiver");

        // TODO Your code..
    }

    protected void delete() {
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
            model.setUserIDField(indexInfo.getUserIdField());
            model.setItemIDField(indexInfo.getItemIdField());
            model.setValueField(indexInfo.getValueField());
            model.setTimestampField(indexInfo.getTimestampField());

            final Map<String, Object> scrollSettings = SettingsUtils.get(
                    modelInfoSettings, "scroll");
            model.setScrollSize(SettingsUtils.get(scrollSettings, "size", 1000));
            model.setScrollKeepAlive(new Scroll(TimeValue
                    .timeValueSeconds(SettingsUtils.get(scrollSettings,
                            "keep_alive", 60))));

            return model;
        } catch (ClassNotFoundException | InstantiationException
                | IllegalAccessException e) {
            throw new TasteSystemException("Could not create an instance of "
                    + className);
        }
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
