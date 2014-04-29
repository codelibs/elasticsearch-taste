package org.codelibs.elasticsearch.taste.recommender;

import java.util.Map;

import org.apache.mahout.cf.taste.common.TasteException;
import org.apache.mahout.cf.taste.eval.RecommenderBuilder;
import org.apache.mahout.cf.taste.model.DataModel;
import org.apache.mahout.cf.taste.recommender.Recommender;
import org.codelibs.elasticsearch.taste.TasteSystemException;
import org.codelibs.elasticsearch.taste.model.IndexInfo;
import org.codelibs.elasticsearch.taste.similarity.SimilarityFactory;
import org.codelibs.elasticsearch.util.SettingsUtils;

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
            throw new TasteSystemException("Could not create an instance of "
                    + factoryName, e);
        }
    }

    @Override
    public abstract Recommender buildRecommender(DataModel dataModel)
            throws TasteException;

}