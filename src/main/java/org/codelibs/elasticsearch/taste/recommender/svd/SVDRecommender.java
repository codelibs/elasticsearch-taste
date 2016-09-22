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

package org.codelibs.elasticsearch.taste.recommender.svd;

import java.io.IOException;
import java.util.Collection;
import java.util.List;

import org.codelibs.elasticsearch.taste.common.FastIDSet;
import org.codelibs.elasticsearch.taste.common.RefreshHelper;
import org.codelibs.elasticsearch.taste.common.Refreshable;
import org.codelibs.elasticsearch.taste.exception.TasteException;
import org.codelibs.elasticsearch.taste.model.DataModel;
import org.codelibs.elasticsearch.taste.model.PreferenceArray;
import org.codelibs.elasticsearch.taste.recommender.AbstractRecommender;
import org.codelibs.elasticsearch.taste.recommender.CandidateItemsStrategy;
import org.codelibs.elasticsearch.taste.recommender.IDRescorer;
import org.codelibs.elasticsearch.taste.recommender.RecommendedItem;
import org.codelibs.elasticsearch.taste.recommender.TopItems;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.base.Preconditions;

/**
 * A {@link org.codelibs.elasticsearch.taste.recommender.Recommender} that uses matrix factorization (a projection of users
 * and items onto a feature space)
 */
public final class SVDRecommender extends AbstractRecommender {

    private Factorization factorization;

    private final Factorizer factorizer;

    private final PersistenceStrategy persistenceStrategy;

    private final RefreshHelper refreshHelper;

    private static final Logger log = LoggerFactory
            .getLogger(SVDRecommender.class);

    public SVDRecommender(final DataModel dataModel, final Factorizer factorizer) {
        this(dataModel, factorizer, getDefaultCandidateItemsStrategy(),
                getDefaultPersistenceStrategy());
    }

    public SVDRecommender(final DataModel dataModel,
            final Factorizer factorizer,
            final CandidateItemsStrategy candidateItemsStrategy) {
        this(dataModel, factorizer, candidateItemsStrategy,
                getDefaultPersistenceStrategy());
    }

    /**
     * Create an SVDRecommender using a persistent store to cache factorizations. A factorization is loaded from the
     * store if present, otherwise a new factorization is computed and saved in the store.
     *
     * The {@link #refresh(java.util.Collection) refresh} method recomputes the factorization and overwrites the store.
     *
     * @param dataModel
     * @param factorizer
     * @param persistenceStrategy
     * @throws IOException
     */
    public SVDRecommender(final DataModel dataModel,
            final Factorizer factorizer,
            final PersistenceStrategy persistenceStrategy) {
        this(dataModel, factorizer, getDefaultCandidateItemsStrategy(),
                persistenceStrategy);
    }

    /**
     * Create an SVDRecommender using a persistent store to cache factorizations. A factorization is loaded from the
     * store if present, otherwise a new factorization is computed and saved in the store.
     *
     * The {@link #refresh(java.util.Collection) refresh} method recomputes the factorization and overwrites the store.
     *
     * @param dataModel
     * @param factorizer
     * @param candidateItemsStrategy
     * @param persistenceStrategy
     *
     */
    public SVDRecommender(final DataModel dataModel,
            final Factorizer factorizer,
            final CandidateItemsStrategy candidateItemsStrategy,
            final PersistenceStrategy persistenceStrategy) {
        super(dataModel, candidateItemsStrategy);
        this.factorizer = Preconditions.checkNotNull(factorizer);
        this.persistenceStrategy = Preconditions
                .checkNotNull(persistenceStrategy);
        try {
            factorization = persistenceStrategy.load();
        } catch (final IOException e) {
            throw new TasteException("Error loading factorization", e);
        }

        if (factorization == null) {
            train();
        }

        refreshHelper = new RefreshHelper(() -> {
            train();
            return null;
        });
        refreshHelper.addDependency(getDataModel());
        refreshHelper.addDependency(factorizer);
        refreshHelper.addDependency(candidateItemsStrategy);
    }

    static PersistenceStrategy getDefaultPersistenceStrategy() {
        return new NoPersistenceStrategy();
    }

    private void train() {
        factorization = factorizer.factorize();
        try {
            persistenceStrategy.maybePersist(factorization);
        } catch (final IOException e) {
            throw new TasteException("Error persisting factorization", e);
        }
    }

    @Override
    public List<RecommendedItem> recommend(final long userID,
            final int howMany, final IDRescorer rescorer) {
        Preconditions.checkArgument(howMany >= 1, "howMany must be at least 1");
        log.debug("Recommending items for user ID '{}'", userID);

        final PreferenceArray preferencesFromUser = getDataModel()
                .getPreferencesFromUser(userID);
        final FastIDSet possibleItemIDs = getAllOtherItems(userID,
                preferencesFromUser);

        final List<RecommendedItem> topItems = TopItems.getTopItems(howMany,
                possibleItemIDs.iterator(), rescorer, new Estimator(userID));
        log.debug("Recommendations are: {}", topItems);

        return topItems;
    }

    /**
     * a preference is estimated by computing the dot-product of the user and item feature vectors
     */
    @Override
    public float estimatePreference(final long userID, final long itemID) {
        final double[] userFeatures = factorization.getUserFeatures(userID);
        final double[] itemFeatures = factorization.getItemFeatures(itemID);
        double estimate = 0;
        for (int feature = 0; feature < userFeatures.length; feature++) {
            estimate += userFeatures[feature] * itemFeatures[feature];
        }
        return (float) estimate;
    }

    private final class Estimator implements TopItems.Estimator<Long> {

        private final long theUserID;

        private Estimator(final long theUserID) {
            this.theUserID = theUserID;
        }

        @Override
        public double estimate(final Long itemID) {
            return estimatePreference(theUserID, itemID);
        }
    }

    /**
     * Refresh the data model and factorization.
     */
    @Override
    public void refresh(final Collection<Refreshable> alreadyRefreshed) {
        refreshHelper.refresh(alreadyRefreshed);
    }

}
