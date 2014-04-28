package org.codelibs.elasticsearch.taste.eval;

import java.util.HashMap;
import java.util.Map;

import org.apache.mahout.cf.taste.common.TasteException;
import org.apache.mahout.cf.taste.impl.recommender.GenericItemBasedRecommender;
import org.apache.mahout.cf.taste.model.DataModel;
import org.apache.mahout.cf.taste.recommender.Recommender;
import org.apache.mahout.cf.taste.similarity.ItemSimilarity;
import org.codelibs.elasticsearch.taste.model.IndexInfo;
import org.codelibs.elasticsearch.util.SettingsUtils;

public class ItemBasedRecommenderBuilder extends AbstractRecommenderBuilder {

    public ItemBasedRecommenderBuilder(final IndexInfo indexInfo,
            final Map<String, Object> rootSettings) {
        super(indexInfo, rootSettings);
    }

    @Override
    public Recommender buildRecommender(final DataModel dataModel)
            throws TasteException {
        final Map<String, Object> similaritySettings = SettingsUtils.get(
                rootSettings, "similarity", new HashMap<String, Object>());
        similaritySettings.put(DATA_MODEL_ATTR, dataModel);
        final ItemSimilarity similarity = createSimilarity(similaritySettings);

        return new GenericItemBasedRecommender(dataModel, similarity);
    }
}
