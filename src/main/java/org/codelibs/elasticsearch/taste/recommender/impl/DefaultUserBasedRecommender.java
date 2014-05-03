package org.codelibs.elasticsearch.taste.recommender.impl;

import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.Callable;

import org.apache.mahout.cf.taste.common.Refreshable;
import org.apache.mahout.cf.taste.common.TasteException;
import org.apache.mahout.cf.taste.impl.common.FastIDSet;
import org.apache.mahout.cf.taste.impl.common.RefreshHelper;
import org.apache.mahout.cf.taste.impl.recommender.AbstractRecommender;
import org.apache.mahout.cf.taste.impl.recommender.EstimatedPreferenceCapper;
import org.apache.mahout.cf.taste.impl.recommender.TopItems;
import org.apache.mahout.cf.taste.model.DataModel;
import org.apache.mahout.cf.taste.neighborhood.UserNeighborhood;
import org.apache.mahout.cf.taste.recommender.IDRescorer;
import org.apache.mahout.cf.taste.recommender.RecommendedItem;
import org.apache.mahout.cf.taste.recommender.Rescorer;
import org.apache.mahout.cf.taste.recommender.UserBasedRecommender;
import org.apache.mahout.cf.taste.similarity.UserSimilarity;
import org.apache.mahout.common.LongPair;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.base.Preconditions;

/**
 * <p>
 * A simple {@link org.apache.mahout.cf.taste.recommender.Recommender}
 * which uses a given {@link DataModel} and {@link UserNeighborhood} to produce recommendations.
 * </p>
 */
public class DefaultUserBasedRecommender extends AbstractRecommender implements
        UserBasedRecommender {

    private static final Logger log = LoggerFactory
            .getLogger(DefaultUserBasedRecommender.class);

    private final UserNeighborhood neighborhood;

    private final UserSimilarity similarity;

    private final RefreshHelper refreshHelper;

    private EstimatedPreferenceCapper capper;

    public DefaultUserBasedRecommender(final DataModel dataModel,
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
            final int howMany, final IDRescorer rescorer) throws TasteException {
        Preconditions.checkArgument(howMany >= 1, "howMany must be at least 1");

        log.debug("Recommending items for user ID '{}'", userID);

        final long[] theNeighborhood = neighborhood.getUserNeighborhood(userID);

        if (theNeighborhood.length == 0) {
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
    public float estimatePreference(final long userID, final long itemID)
            throws TasteException {
        final DataModel model = getDataModel();
        final Float actualPref = model.getPreferenceValue(userID, itemID);
        if (actualPref != null) {
            if (log.isDebugEnabled()) {
                log.debug(
                        "Not estimated. User {} has actual value {} for Item {}.",
                        userID, actualPref, itemID);
            }
            return actualPref;
        }
        final long[] theNeighborhood = neighborhood.getUserNeighborhood(userID);
        return doEstimatePreference(userID, theNeighborhood, itemID);
    }

    @Override
    public long[] mostSimilarUserIDs(final long userID, final int howMany)
            throws TasteException {
        return mostSimilarUserIDs(userID, howMany, null);
    }

    @Override
    public long[] mostSimilarUserIDs(final long userID, final int howMany,
            final Rescorer<LongPair> rescorer) throws TasteException {
        final TopItems.Estimator<Long> estimator = new MostSimilarEstimator(
                userID, similarity, rescorer);
        return doMostSimilarUsers(howMany, estimator);
    }

    private long[] doMostSimilarUsers(final int howMany,
            final TopItems.Estimator<Long> estimator) throws TasteException {
        final DataModel model = getDataModel();
        return TopItems.getTopUsers(howMany, model.getUserIDs(), null,
                estimator);
    }

    protected float doEstimatePreference(final long theUserID,
            final long[] theNeighborhood, final long itemID)
            throws TasteException {
        if (theNeighborhood.length == 0) {
            if (log.isDebugEnabled()) {
                log.debug("User {} does not have no neighborhood.", theUserID);
            }
            return Float.NaN;
        }

        if (log.isDebugEnabled()) {
            log.debug("User {} has {} neighborhood users.", theUserID,
                    theNeighborhood.length);
        }

        final DataModel dataModel = getDataModel();
        double preference = 0.0;
        double totalSimilarity = 0.0;
        int count = 0;
        for (final long userID : theNeighborhood) {
            if (userID != theUserID) {
                // See GenericItemBasedRecommender.doEstimatePreference() too
                final Float pref = dataModel.getPreferenceValue(userID, itemID);
                if (pref != null) {
                    final double theSimilarity = similarity.userSimilarity(
                            theUserID, userID);
                    if (!Double.isNaN(theSimilarity)) {
                        preference += theSimilarity * pref;
                        totalSimilarity += theSimilarity;
                        count++;
                        if (log.isDebugEnabled()) {
                            log.debug(
                                    "User {}->{}, Item {}, Prefernce {}, Similarity {}",
                                    theUserID, userID, itemID, pref.toString(),
                                    theSimilarity);
                        }
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
            if (log.isDebugEnabled()) {
                log.debug(
                        "Count for User {} and item {} is {}. Estimate is {}.",
                        theUserID, itemID, count,
                        (float) (preference / totalSimilarity));
            }
            return Float.NaN;
        }
        float estimate = (float) (preference / totalSimilarity);
        if (capper != null) {
            estimate = capper.capEstimate(estimate);
        }
        if (log.isDebugEnabled()) {
            log.debug("Estimate for User {} and item {} is {}.", theUserID,
                    itemID, estimate);
        }
        return estimate;
    }

    protected FastIDSet getAllOtherItems(final long[] theNeighborhood,
            final long theUserID) throws TasteException {
        final DataModel dataModel = getDataModel();
        final FastIDSet possibleItemIDs = new FastIDSet();
        for (final long userID : theNeighborhood) {
            possibleItemIDs.addAll(dataModel.getItemIDsFromUser(userID));
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
        public double estimate(final Long userID) throws TasteException {
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

        private final long[] theNeighborhood;

        Estimator(final long theUserID, final long[] theNeighborhood) {
            this.theUserID = theUserID;
            this.theNeighborhood = theNeighborhood;
        }

        @Override
        public double estimate(final Long itemID) throws TasteException {
            return doEstimatePreference(theUserID, theNeighborhood, itemID);
        }
    }
}
