package org.codelibs.elasticsearch.taste.recommender;

import java.util.Map;

import org.codelibs.elasticsearch.taste.eval.RecommenderBuilder;
import org.codelibs.elasticsearch.taste.exception.TasteException;
import org.codelibs.elasticsearch.taste.model.DataModel;
import org.codelibs.elasticsearch.taste.model.IndexInfo;
import org.codelibs.elasticsearch.taste.similarity.SimilarityFactory;
import org.codelibs.elasticsearch.taste.util.SettingsUtils;

public abstract class AbstractRecommenderBuilder implements RecommenderBuilder {
    protected static final String DATA_MODEL_ATTR = "dataModel";

    protected static final String USER_SIMILARITY_ATTR = "userSimilarity";

    protected IndexInfo indexInfo;

    protected Map<String, Object> rootSettings;

    public AbstractRecommenderBuilder(final IndexInfo indexInfo,
            final Map<String, Object> rootSettings) {
        this.indexInfo = indexInfo;
        this.rootSettings = rootSettings;
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
            throw new TasteException("Could not create an instance of "
                    + factoryName, e);
        }
    }

    @Override
    public abstract Recommender buildRecommender(DataModel dataModel);

}