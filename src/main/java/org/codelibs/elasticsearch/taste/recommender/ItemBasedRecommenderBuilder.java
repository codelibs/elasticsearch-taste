package org.codelibs.elasticsearch.taste.recommender;

import java.util.HashMap;
import java.util.Map;

import org.codelibs.elasticsearch.taste.model.DataModel;
import org.codelibs.elasticsearch.taste.model.IndexInfo;
import org.codelibs.elasticsearch.taste.similarity.ItemSimilarity;
import org.codelibs.elasticsearch.taste.util.SettingsUtils;

public class ItemBasedRecommenderBuilder extends AbstractRecommenderBuilder {

    public ItemBasedRecommenderBuilder(final IndexInfo indexInfo,
            final Map<String, Object> rootSettings) {
        super(indexInfo, rootSettings);
    }

    @Override
    public Recommender buildRecommender(final DataModel dataModel) {
        final Map<String, Object> similaritySettings = SettingsUtils.get(
                rootSettings, "similarity", new HashMap<String, Object>());
        similaritySettings.put(DATA_MODEL_ATTR, dataModel);
        final ItemSimilarity similarity = createSimilarity(similaritySettings);

        return new GenericItemBasedRecommender(dataModel, similarity);
    }
}
