package org.codelibs.elasticsearch.taste.recommender;

import java.util.HashMap;
import java.util.Map;

import org.codelibs.elasticsearch.taste.exception.TasteException;
import org.codelibs.elasticsearch.taste.model.DataModel;
import org.codelibs.elasticsearch.taste.model.IndexInfo;
import org.codelibs.elasticsearch.taste.neighborhood.UserNeighborhood;
import org.codelibs.elasticsearch.taste.neighborhood.UserNeighborhoodFactory;
import org.codelibs.elasticsearch.taste.similarity.UserSimilarity;
import org.codelibs.elasticsearch.util.settings.SettingsUtils;

public class UserBasedRecommenderBuilder extends AbstractRecommenderBuilder {

    public UserBasedRecommenderBuilder(final IndexInfo indexInfo,
            final Map<String, Object> rootSettings) {
        super(indexInfo, rootSettings);
    }

    @Override
    public Recommender buildRecommender(final DataModel dataModel) {
        final Map<String, Object> similaritySettings = SettingsUtils.get(
                rootSettings, "similarity", new HashMap<String, Object>());
        similaritySettings.put(DATA_MODEL_ATTR, dataModel);
        final UserSimilarity similarity = createSimilarity(similaritySettings);

        final Map<String, Object> neighborhoodSettings = SettingsUtils.get(
                rootSettings, "neighborhood", new HashMap<String, Object>());
        neighborhoodSettings.put(DATA_MODEL_ATTR, dataModel);
        neighborhoodSettings.put(USER_SIMILARITY_ATTR, similarity);
        final UserNeighborhood neighborhood = createUserNeighborhood(neighborhoodSettings);

        return new GenericUserBasedRecommender(dataModel, neighborhood,
                similarity);
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
            throw new TasteException("Could not create an instance of "
                    + factoryName, e);
        }
    }
}
