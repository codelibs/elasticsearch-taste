/**
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.codelibs.elasticsearch.taste.recommender;

import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.Callable;

import org.codelibs.elasticsearch.taste.common.FastIDSet;
import org.codelibs.elasticsearch.taste.common.FullRunningAverage;
import org.codelibs.elasticsearch.taste.common.LongPair;
import org.codelibs.elasticsearch.taste.common.RefreshHelper;
import org.codelibs.elasticsearch.taste.common.Refreshable;
import org.codelibs.elasticsearch.taste.common.RunningAverage;
import org.codelibs.elasticsearch.taste.model.DataModel;
import org.codelibs.elasticsearch.taste.model.PreferenceArray;
import org.codelibs.elasticsearch.taste.similarity.ItemSimilarity;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.base.Preconditions;

/**
 * <p>
 * A simple {@link org.codelibs.elasticsearch.taste.recommender.Recommender} which uses a given
 * {@link org.codelibs.elasticsearch.taste.model.DataModel} and
 * {@link org.codelibs.elasticsearch.taste.similarity.ItemSimilarity} to produce recommendations. This class
 * represents Taste's support for item-based recommenders.
 * </p>
 *
 * <p>
 * The {@link org.codelibs.elasticsearch.taste.similarity.ItemSimilarity} is the most important point to discuss
 * here. Item-based recommenders are useful because they can take advantage of something to be very fast: they
 * base their computations on item similarity, not user similarity, and item similarity is relatively static.
 * It can be precomputed, instead of re-computed in real time.
 * </p>
 *
 * <p>
 * Thus it's strongly recommended that you use
 * {@link org.codelibs.elasticsearch.taste.similarity.GenericItemSimilarity} with pre-computed similarities if
 * you're going to use this class. You can use
 * {@link org.codelibs.elasticsearch.taste.similarity.PearsonCorrelationSimilarity} too, which computes
 * similarities in real-time, but will probably find this painfully slow for large amounts of data.
 * </p>
 */
public class GenericItemBasedRecommender extends AbstractRecommender implements
        ItemBasedRecommender {

    private static final Logger log = LoggerFactory
            .getLogger(GenericItemBasedRecommender.class);

    private final ItemSimilarity similarity;

    private final MostSimilarItemsCandidateItemsStrategy mostSimilarItemsCandidateItemsStrategy;

    private final RefreshHelper refreshHelper;

    private EstimatedPreferenceCapper capper;

    private static final boolean EXCLUDE_ITEM_IF_NOT_SIMILAR_TO_ALL_BY_DEFAULT = true;

    public GenericItemBasedRecommender(
            final DataModel dataModel,
            final ItemSimilarity similarity,
            final CandidateItemsStrategy candidateItemsStrategy,
            final MostSimilarItemsCandidateItemsStrategy mostSimilarItemsCandidateItemsStrategy) {
        super(dataModel, candidateItemsStrategy);
        Preconditions.checkArgument(similarity != null, "similarity is null");
        this.similarity = similarity;
        Preconditions.checkArgument(
                mostSimilarItemsCandidateItemsStrategy != null,
                "mostSimilarItemsCandidateItemsStrategy is null");
        this.mostSimilarItemsCandidateItemsStrategy = mostSimilarItemsCandidateItemsStrategy;
        refreshHelper = new RefreshHelper(new Callable<Void>() {
            @Override
            public Void call() {
                capper = buildCapper();
                return null;
            }
        });
        refreshHelper.addDependency(dataModel);
        refreshHelper.addDependency(similarity);
        refreshHelper.addDependency(candidateItemsStrategy);
        refreshHelper.addDependency(mostSimilarItemsCandidateItemsStrategy);
        capper = buildCapper();
    }

    public GenericItemBasedRecommender(final DataModel dataModel,
            final ItemSimilarity similarity) {
        this(dataModel, similarity, AbstractRecommender
                .getDefaultCandidateItemsStrategy(),
                getDefaultMostSimilarItemsCandidateItemsStrategy());
    }

    protected static MostSimilarItemsCandidateItemsStrategy getDefaultMostSimilarItemsCandidateItemsStrategy() {
        return new PreferredItemsNeighborhoodCandidateItemsStrategy();
    }

    public ItemSimilarity getSimilarity() {
        return similarity;
    }

    @Override
    public List<RecommendedItem> recommend(final long userID,
            final int howMany, final IDRescorer rescorer) {
        Preconditions.checkArgument(howMany >= 1, "howMany must be at least 1");
        log.debug("Recommending items for user ID '{}'", userID);

        final PreferenceArray preferencesFromUser = getDataModel()
                .getPreferencesFromUser(userID);
        if (preferencesFromUser.length() == 0) {
            return Collections.emptyList();
        }

        final FastIDSet possibleItemIDs = getAllOtherItems(userID,
                preferencesFromUser);

        final TopItems.Estimator<Long> estimator = new Estimator(userID,
                preferencesFromUser);

        final List<RecommendedItem> topItems = TopItems.getTopItems(howMany,
                possibleItemIDs.iterator(), rescorer, estimator);

        log.debug("Recommendations are: {}", topItems);
        return topItems;
    }

    @Override
    public float estimatePreference(final long userID, final long itemID) {
        final PreferenceArray preferencesFromUser = getDataModel()
                .getPreferencesFromUser(userID);
        final Float actualPref = getPreferenceForItem(preferencesFromUser,
                itemID);
        if (actualPref != null) {
            return actualPref;
        }
        return doEstimatePreference(userID, preferencesFromUser, itemID);
    }

    private static Float getPreferenceForItem(
            final PreferenceArray preferencesFromUser, final long itemID) {
        final int size = preferencesFromUser.length();
        for (int i = 0; i < size; i++) {
            if (preferencesFromUser.getItemID(i) == itemID) {
                return preferencesFromUser.getValue(i);
            }
        }
        return null;
    }

    @Override
    public List<RecommendedItem> mostSimilarItems(final long itemID,
            final int howMany) {
        return mostSimilarItems(itemID, howMany, null);
    }

    @Override
    public List<RecommendedItem> mostSimilarItems(final long itemID,
            final int howMany, final Rescorer<LongPair> rescorer) {
        final TopItems.Estimator<Long> estimator = new MostSimilarEstimator(
                itemID, similarity, rescorer);
        return doMostSimilarItems(new long[] { itemID }, howMany, estimator);
    }

    @Override
    public List<RecommendedItem> mostSimilarItems(final long[] itemIDs,
            final int howMany) {
        final TopItems.Estimator<Long> estimator = new MultiMostSimilarEstimator(
                itemIDs, similarity, null,
                EXCLUDE_ITEM_IF_NOT_SIMILAR_TO_ALL_BY_DEFAULT);
        return doMostSimilarItems(itemIDs, howMany, estimator);
    }

    @Override
    public List<RecommendedItem> mostSimilarItems(final long[] itemIDs,
            final int howMany, final Rescorer<LongPair> rescorer) {
        final TopItems.Estimator<Long> estimator = new MultiMostSimilarEstimator(
                itemIDs, similarity, rescorer,
                EXCLUDE_ITEM_IF_NOT_SIMILAR_TO_ALL_BY_DEFAULT);
        return doMostSimilarItems(itemIDs, howMany, estimator);
    }

    @Override
    public List<RecommendedItem> mostSimilarItems(final long[] itemIDs,
            final int howMany, final boolean excludeItemIfNotSimilarToAll) {
        final TopItems.Estimator<Long> estimator = new MultiMostSimilarEstimator(
                itemIDs, similarity, null, excludeItemIfNotSimilarToAll);
        return doMostSimilarItems(itemIDs, howMany, estimator);
    }

    @Override
    public List<RecommendedItem> mostSimilarItems(final long[] itemIDs,
            final int howMany, final Rescorer<LongPair> rescorer,
            final boolean excludeItemIfNotSimilarToAll) {
        final TopItems.Estimator<Long> estimator = new MultiMostSimilarEstimator(
                itemIDs, similarity, rescorer, excludeItemIfNotSimilarToAll);
        return doMostSimilarItems(itemIDs, howMany, estimator);
    }

    @Override
    public List<RecommendedItem> recommendedBecause(final long userID,
            final long itemID, final int howMany) {
        Preconditions.checkArgument(howMany >= 1, "howMany must be at least 1");

        final DataModel model = getDataModel();
        final TopItems.Estimator<Long> estimator = new RecommendedBecauseEstimator(
                userID, itemID);

        final PreferenceArray prefs = model.getPreferencesFromUser(userID);
        final int size = prefs.length();
        final FastIDSet allUserItems = new FastIDSet(size);
        for (int i = 0; i < size; i++) {
            allUserItems.add(prefs.getItemID(i));
        }
        allUserItems.remove(itemID);

        return TopItems.getTopItems(howMany, allUserItems.iterator(), null,
                estimator);
    }

    private List<RecommendedItem> doMostSimilarItems(final long[] itemIDs,
            final int howMany, final TopItems.Estimator<Long> estimator) {
        final FastIDSet possibleItemIDs = mostSimilarItemsCandidateItemsStrategy
                .getCandidateItems(itemIDs, getDataModel());
        return TopItems.getTopItems(howMany, possibleItemIDs.iterator(), null,
                estimator);
    }

    protected float doEstimatePreference(final long userID,
            final PreferenceArray preferencesFromUser, final long itemID) {
        double preference = 0.0;
        double totalSimilarity = 0.0;
        int count = 0;
        final double[] similarities = similarity.itemSimilarities(itemID,
                preferencesFromUser.getIDs());
        for (int i = 0; i < similarities.length; i++) {
            final double theSimilarity = similarities[i];
            if (!Double.isNaN(theSimilarity)) {
                // Weights can be negative!
                preference += theSimilarity * preferencesFromUser.getValue(i);
                totalSimilarity += theSimilarity;
                count++;
            }
        }
        // Throw out the estimate if it was based on no data points, of course, but also if based on
        // just one. This is a bit of a band-aid on the 'stock' item-based algorithm for the moment.
        // The reason is that in this case the estimate is, simply, the user's rating for one item
        // that happened to have a defined similarity. The similarity score doesn't matter, and that
        // seems like a bad situation.
        if (count <= 1) {
            return Float.NaN;
        }
        float estimate = (float) (preference / totalSimilarity);
        if (capper != null) {
            estimate = capper.capEstimate(estimate);
        }
        return estimate;
    }

    @Override
    public void refresh(final Collection<Refreshable> alreadyRefreshed) {
        refreshHelper.refresh(alreadyRefreshed);
    }

    @Override
    public String toString() {
        return "GenericItemBasedRecommender[similarity:" + similarity + ']';
    }

    private EstimatedPreferenceCapper buildCapper() {
        final DataModel dataModel = getDataModel();
        if (Float.isNaN(dataModel.getMinPreference())
                && Float.isNaN(dataModel.getMaxPreference())) {
            return null;
        } else {
            return new EstimatedPreferenceCapper(dataModel);
        }
    }

    public static class MostSimilarEstimator implements
            TopItems.Estimator<Long> {

        private final long toItemID;

        private final ItemSimilarity similarity;

        private final Rescorer<LongPair> rescorer;

        public MostSimilarEstimator(final long toItemID,
                final ItemSimilarity similarity,
                final Rescorer<LongPair> rescorer) {
            this.toItemID = toItemID;
            this.similarity = similarity;
            this.rescorer = rescorer;
        }

        @Override
        public double estimate(final Long itemID) {
            final LongPair pair = new LongPair(toItemID, itemID);
            if (rescorer != null && rescorer.isFiltered(pair)) {
                return Double.NaN;
            }
            final double originalEstimate = similarity.itemSimilarity(toItemID,
                    itemID);
            return rescorer == null ? originalEstimate : rescorer.rescore(pair,
                    originalEstimate);
        }
    }

    private final class Estimator implements TopItems.Estimator<Long> {

        private final long userID;

        private final PreferenceArray preferencesFromUser;

        private Estimator(final long userID,
                final PreferenceArray preferencesFromUser) {
            this.userID = userID;
            this.preferencesFromUser = preferencesFromUser;
        }

        @Override
        public double estimate(final Long itemID) {
            return doEstimatePreference(userID, preferencesFromUser, itemID);
        }
    }

    private static final class MultiMostSimilarEstimator implements
            TopItems.Estimator<Long> {

        private final long[] toItemIDs;

        private final ItemSimilarity similarity;

        private final Rescorer<LongPair> rescorer;

        private final boolean excludeItemIfNotSimilarToAll;

        private MultiMostSimilarEstimator(final long[] toItemIDs,
                final ItemSimilarity similarity,
                final Rescorer<LongPair> rescorer,
                final boolean excludeItemIfNotSimilarToAll) {
            this.toItemIDs = toItemIDs;
            this.similarity = similarity;
            this.rescorer = rescorer;
            this.excludeItemIfNotSimilarToAll = excludeItemIfNotSimilarToAll;
        }

        @Override
        public double estimate(final Long itemID) {
            final RunningAverage average = new FullRunningAverage();
            final double[] similarities = similarity.itemSimilarities(itemID,
                    toItemIDs);
            for (int i = 0; i < toItemIDs.length; i++) {
                final long toItemID = toItemIDs[i];
                final LongPair pair = new LongPair(toItemID, itemID);
                if (rescorer != null && rescorer.isFiltered(pair)) {
                    continue;
                }
                double estimate = similarities[i];
                if (rescorer != null) {
                    estimate = rescorer.rescore(pair, estimate);
                }
                if (excludeItemIfNotSimilarToAll || !Double.isNaN(estimate)) {
                    average.addDatum(estimate);
                }
            }
            final double averageEstimate = average.getAverage();
            return averageEstimate == 0 ? Double.NaN : averageEstimate;
        }
    }

    private final class RecommendedBecauseEstimator implements
            TopItems.Estimator<Long> {

        private final long userID;

        private final long recommendedItemID;

        private RecommendedBecauseEstimator(final long userID,
                final long recommendedItemID) {
            this.userID = userID;
            this.recommendedItemID = recommendedItemID;
        }

        @Override
        public double estimate(final Long itemID) {
            final Float pref = getDataModel()
                    .getPreferenceValue(userID, itemID);
            if (pref == null) {
                return Float.NaN;
            }
            final double similarityValue = similarity.itemSimilarity(
                    recommendedItemID, itemID);
            return (1.0 + similarityValue) * pref;
        }
    }

}
