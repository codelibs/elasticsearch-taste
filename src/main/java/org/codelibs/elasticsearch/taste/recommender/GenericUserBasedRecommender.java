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
import org.codelibs.elasticsearch.taste.common.LongPair;
import org.codelibs.elasticsearch.taste.common.RefreshHelper;
import org.codelibs.elasticsearch.taste.common.Refreshable;
import org.codelibs.elasticsearch.taste.model.DataModel;
import org.codelibs.elasticsearch.taste.neighborhood.UserNeighborhood;
import org.codelibs.elasticsearch.taste.similarity.UserSimilarity;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.base.Preconditions;

/**
 * <p>
 * A simple {@link org.codelibs.elasticsearch.taste.recommender.Recommender}
 * which uses a given {@link DataModel} and {@link UserNeighborhood} to produce recommendations.
 * </p>
 */
public class GenericUserBasedRecommender extends AbstractRecommender implements
        UserBasedRecommender {

    private static final Logger log = LoggerFactory
            .getLogger(GenericUserBasedRecommender.class);

    private final UserNeighborhood neighborhood;

    private final UserSimilarity similarity;

    private final RefreshHelper refreshHelper;

    private EstimatedPreferenceCapper capper;

    public GenericUserBasedRecommender(final DataModel dataModel,
            final UserNeighborhood neighborhood, final UserSimilarity similarity) {
        super(dataModel);
        Preconditions.checkArgument(neighborhood != null,
                "neighborhood is null");
        this.neighborhood = neighborhood;
        this.similarity = similarity;
        refreshHelper = new RefreshHelper(new Callable<Void>() {
            @Override
            public Void call() {
                capper = buildCapper();
                return null;
            }
        });
        refreshHelper.addDependency(dataModel);
        refreshHelper.addDependency(similarity);
        refreshHelper.addDependency(neighborhood);
        capper = buildCapper();
    }

    public UserSimilarity getSimilarity() {
        return similarity;
    }

    @Override
    public List<RecommendedItem> recommend(final long userID,
            final int howMany, final IDRescorer rescorer) {
        Preconditions.checkArgument(howMany >= 1, "howMany must be at least 1");

        log.debug("Recommending items for user ID '{}'", userID);

        final List<SimilarUser> theNeighborhood = neighborhood
                .getUserNeighborhood(userID);

        if (theNeighborhood.size() == 0) {
            return Collections.emptyList();
        }

        final FastIDSet allItemIDs = getAllOtherItems(theNeighborhood, userID);

        final TopItems.Estimator<Long> estimator = new Estimator(userID,
                theNeighborhood);

        final List<RecommendedItem> topItems = TopItems.getTopItems(howMany,
                allItemIDs.iterator(), rescorer, estimator);

        log.debug("Recommendations are: {}", topItems);
        return topItems;
    }

    @Override
    public float estimatePreference(final long userID, final long itemID) {
        final DataModel model = getDataModel();
        final Float actualPref = model.getPreferenceValue(userID, itemID);
        if (actualPref != null) {
            return actualPref;
        }
        final List<SimilarUser> theNeighborhood = neighborhood
                .getUserNeighborhood(userID);
        return doEstimatePreference(userID, theNeighborhood, itemID);
    }

    @Override
    public List<SimilarUser> mostSimilarUserIDs(final long userID,
            final int howMany) {
        return mostSimilarUserIDs(userID, howMany, null);
    }

    @Override
    public List<SimilarUser> mostSimilarUserIDs(final long userID,
            final int howMany, final Rescorer<LongPair> rescorer) {
        final TopItems.Estimator<Long> estimator = new MostSimilarEstimator(
                userID, similarity, rescorer);
        return doMostSimilarUsers(howMany, estimator);
            }

    private List<SimilarUser> doMostSimilarUsers(final int howMany,
            final TopItems.Estimator<Long> estimator) {
        final DataModel model = getDataModel();
        return TopItems.getTopUsers(howMany, model.getUserIDs(), null,
                estimator);
    }

    protected float doEstimatePreference(final long theUserID,
            final List<SimilarUser> theNeighborhood, final long itemID) {
        if (theNeighborhood.size() == 0) {
            return Float.NaN;
        }
        final DataModel dataModel = getDataModel();
        double preference = 0.0;
        double totalSimilarity = 0.0;
        int count = 0;
        for (final SimilarUser similarUser : theNeighborhood) {
            if (similarUser.getUserID() != theUserID) {
                // See GenericItemBasedRecommender.doEstimatePreference() too
                final Float pref = dataModel.getPreferenceValue(
                        similarUser.getUserID(), itemID);
                if (pref != null) {
                    final double theSimilarity = similarity.userSimilarity(
                            theUserID, similarUser.getUserID());
                    if (!Double.isNaN(theSimilarity)) {
                        preference += theSimilarity * pref;
                        totalSimilarity += theSimilarity;
                        count++;
                    }
                }
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

    protected FastIDSet getAllOtherItems(
            final List<SimilarUser> theNeighborhood, final long theUserID) {
        final DataModel dataModel = getDataModel();
        final FastIDSet possibleItemIDs = new FastIDSet();
        for (final SimilarUser similarUser : theNeighborhood) {
            possibleItemIDs.addAll(dataModel.getItemIDsFromUser(similarUser
                    .getUserID()));
        }
        possibleItemIDs.removeAll(dataModel.getItemIDsFromUser(theUserID));
        return possibleItemIDs;
    }

    @Override
    public void refresh(final Collection<Refreshable> alreadyRefreshed) {
        refreshHelper.refresh(alreadyRefreshed);
    }

    @Override
    public String toString() {
        return "GenericUserBasedRecommender[neighborhood:" + neighborhood + ']';
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

    private static final class MostSimilarEstimator implements
            TopItems.Estimator<Long> {

        private final long toUserID;

        private final UserSimilarity similarity;

        private final Rescorer<LongPair> rescorer;

        private MostSimilarEstimator(final long toUserID,
                final UserSimilarity similarity,
                final Rescorer<LongPair> rescorer) {
            this.toUserID = toUserID;
            this.similarity = similarity;
            this.rescorer = rescorer;
        }

        @Override
        public double estimate(final Long userID) {
            // Don't consider the user itself as a possible most similar user
            if (userID == toUserID) {
                return Double.NaN;
            }
            if (rescorer == null) {
                return similarity.userSimilarity(toUserID, userID);
            } else {
                final LongPair pair = new LongPair(toUserID, userID);
                if (rescorer.isFiltered(pair)) {
                    return Double.NaN;
                }
                final double originalEstimate = similarity.userSimilarity(
                        toUserID, userID);
                return rescorer.rescore(pair, originalEstimate);
            }
        }
    }

    private final class Estimator implements TopItems.Estimator<Long> {

        private final long theUserID;

        private final List<SimilarUser> theNeighborhood;

        Estimator(final long theUserID, final List<SimilarUser> theNeighborhood) {
            this.theUserID = theUserID;
            this.theNeighborhood = theNeighborhood;
        }

        @Override
        public double estimate(final Long itemID) {
            return doEstimatePreference(theUserID, theNeighborhood, itemID);
        }
    }
}
