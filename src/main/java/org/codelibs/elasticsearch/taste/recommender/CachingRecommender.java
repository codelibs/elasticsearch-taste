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

import org.codelibs.elasticsearch.taste.common.Cache;
import org.codelibs.elasticsearch.taste.common.LongPair;
import org.codelibs.elasticsearch.taste.common.RefreshHelper;
import org.codelibs.elasticsearch.taste.common.Refreshable;
import org.codelibs.elasticsearch.taste.common.Retriever;
import org.codelibs.elasticsearch.taste.model.DataModel;
import org.codelibs.elasticsearch.taste.model.PlusAnonymousUserDataModel;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.base.Preconditions;

/**
 * <p>
 * A {@link Recommender} which caches the results from another {@link Recommender} in memory.
 * </p>
 */
public final class CachingRecommender implements Recommender {

    private static final Logger log = LoggerFactory
            .getLogger(CachingRecommender.class);

    private final Recommender recommender;

    private final int[] maxHowMany;

    private final Retriever<Long, Recommendations> recommendationsRetriever;

    private final Cache<Long, Recommendations> recommendationCache;

    private final Cache<LongPair, Float> estimatedPrefCache;

    private final RefreshHelper refreshHelper;

    private IDRescorer currentRescorer;

    public CachingRecommender(final Recommender recommender) {
        Preconditions.checkArgument(recommender != null, "recommender is null");
        this.recommender = recommender;
        maxHowMany = new int[] { 1 };
        // Use "num users" as an upper limit on cache size. Rough guess.
        final int numUsers = recommender.getDataModel().getNumUsers();
        recommendationsRetriever = new RecommendationRetriever();
        recommendationCache = new Cache<Long, Recommendations>(
                recommendationsRetriever, numUsers);
        estimatedPrefCache = new Cache<LongPair, Float>(
                new EstimatedPrefRetriever(), numUsers);
        refreshHelper = new RefreshHelper(new Callable<Object>() {
            @Override
            public Object call() {
                clear();
                return null;
            }
        });
        refreshHelper.addDependency(recommender);
    }

    private void setCurrentRescorer(final IDRescorer rescorer) {
        if (rescorer == null) {
            if (currentRescorer != null) {
                currentRescorer = null;
                clear();
            }
        } else {
            if (!rescorer.equals(currentRescorer)) {
                currentRescorer = rescorer;
                clear();
            }
        }
    }

    @Override
    public List<RecommendedItem> recommend(final long userID, final int howMany) {
        return recommend(userID, howMany, null);
    }

    @Override
    public List<RecommendedItem> recommend(final long userID,
            final int howMany, final IDRescorer rescorer) {
        Preconditions.checkArgument(howMany >= 1, "howMany must be at least 1");
        synchronized (maxHowMany) {
            if (howMany > maxHowMany[0]) {
                maxHowMany[0] = howMany;
            }
        }

        // Special case, avoid caching an anonymous user
        if (userID == PlusAnonymousUserDataModel.TEMP_USER_ID) {
            return recommendationsRetriever.get(
                    PlusAnonymousUserDataModel.TEMP_USER_ID).getItems();
        }

        setCurrentRescorer(rescorer);

        Recommendations recommendations = recommendationCache.get(userID);
        if (recommendations.getItems().size() < howMany
                && !recommendations.isNoMoreRecommendableItems()) {
            clear(userID);
            recommendations = recommendationCache.get(userID);
            if (recommendations.getItems().size() < howMany) {
                recommendations.setNoMoreRecommendableItems(true);
            }
        }

        final List<RecommendedItem> recommendedItems = recommendations
                .getItems();
        return recommendedItems.size() > howMany ? recommendedItems.subList(0,
                howMany) : recommendedItems;
    }

    @Override
    public float estimatePreference(final long userID, final long itemID) {
        return estimatedPrefCache.get(new LongPair(userID, itemID));
    }

    @Override
    public void setPreference(final long userID, final long itemID,
            final float value) {
        recommender.setPreference(userID, itemID, value);
        clear(userID);
    }

    @Override
    public void removePreference(final long userID, final long itemID) {
        recommender.removePreference(userID, itemID);
        clear(userID);
    }

    @Override
    public DataModel getDataModel() {
        return recommender.getDataModel();
    }

    @Override
    public void refresh(final Collection<Refreshable> alreadyRefreshed) {
        refreshHelper.refresh(alreadyRefreshed);
    }

    /**
     * <p>
     * Clears cached recommendations for the given user.
     * </p>
     *
     * @param userID
     *          clear cached data associated with this user ID
     */
    public void clear(final long userID) {
        log.debug("Clearing recommendations for user ID '{}'", userID);
        recommendationCache.remove(userID);
        estimatedPrefCache
                .removeKeysMatching(new Cache.MatchPredicate<LongPair>() {
                    @Override
                    public boolean matches(final LongPair userItemPair) {
                        return userItemPair.getFirst() == userID;
                    }
                });
    }

    /**
     * <p>
     * Clears all cached recommendations.
     * </p>
     */
    public void clear() {
        log.debug("Clearing all recommendations...");
        recommendationCache.clear();
        estimatedPrefCache.clear();
    }

    @Override
    public String toString() {
        return "CachingRecommender[recommender:" + recommender + ']';
    }

    private final class RecommendationRetriever implements
            Retriever<Long, Recommendations> {
        @Override
        public Recommendations get(final Long key) {
            log.debug("Retrieving new recommendations for user ID '{}'", key);
            final int howMany = maxHowMany[0];
            final IDRescorer rescorer = currentRescorer;
            final List<RecommendedItem> recommendations = rescorer == null ? recommender
                    .recommend(key, howMany) : recommender.recommend(key,
                    howMany, rescorer);
            return new Recommendations(
                    Collections.unmodifiableList(recommendations));
        }
    }

    private final class EstimatedPrefRetriever implements
            Retriever<LongPair, Float> {
        @Override
        public Float get(final LongPair key) {
            final long userID = key.getFirst();
            final long itemID = key.getSecond();
            log.debug(
                    "Retrieving estimated preference for user ID '{}' and item ID '{}'",
                    userID, itemID);
            return recommender.estimatePreference(userID, itemID);
        }
    }

    private static final class Recommendations {

        private final List<RecommendedItem> items;

        private boolean noMoreRecommendableItems;

        private Recommendations(final List<RecommendedItem> items) {
            this.items = items;
        }

        List<RecommendedItem> getItems() {
            return items;
        }

        boolean isNoMoreRecommendableItems() {
            return noMoreRecommendableItems;
        }

        void setNoMoreRecommendableItems(final boolean noMoreRecommendableItems) {
            this.noMoreRecommendableItems = noMoreRecommendableItems;
        }
    }

}
