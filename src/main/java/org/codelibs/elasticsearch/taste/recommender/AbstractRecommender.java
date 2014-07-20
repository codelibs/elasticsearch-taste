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

import java.util.List;

import org.codelibs.elasticsearch.taste.common.FastIDSet;
import org.codelibs.elasticsearch.taste.model.DataModel;
import org.codelibs.elasticsearch.taste.model.PreferenceArray;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.base.Preconditions;

public abstract class AbstractRecommender implements Recommender {

    private static final Logger log = LoggerFactory
            .getLogger(AbstractRecommender.class);

    private final DataModel dataModel;

    private final CandidateItemsStrategy candidateItemsStrategy;

    protected AbstractRecommender(final DataModel dataModel,
            final CandidateItemsStrategy candidateItemsStrategy) {
        this.dataModel = Preconditions.checkNotNull(dataModel);
        this.candidateItemsStrategy = Preconditions
                .checkNotNull(candidateItemsStrategy);
    }

    protected AbstractRecommender(final DataModel dataModel) {
        this(dataModel, getDefaultCandidateItemsStrategy());
    }

    protected static CandidateItemsStrategy getDefaultCandidateItemsStrategy() {
        return new PreferredItemsNeighborhoodCandidateItemsStrategy();
    }

    /**
     * <p>
     * Default implementation which just calls
     * {@link Recommender#recommend(long, int, org.codelibs.elasticsearch.taste.recommender.IDRescorer)}, with a
     * {@link org.codelibs.elasticsearch.taste.recommender.Rescorer} that does nothing.
     * </p>
     */
    @Override
    public List<RecommendedItem> recommend(final long userID, final int howMany) {
        return recommend(userID, howMany, null);
    }

    /**
     * <p>
     * Default implementation which just calls {@link DataModel#setPreference(long, long, float)}.
     * </p>
     *
     * @throws IllegalArgumentException
     *           if userID or itemID is {@code null}, or if value is {@link Double#NaN}
     */
    @Override
    public void setPreference(final long userID, final long itemID,
            final float value) {
        Preconditions.checkArgument(!Float.isNaN(value), "NaN value");
        log.debug("Setting preference for user {}, item {}", userID, itemID);
        dataModel.setPreference(userID, itemID, value);
    }

    /**
     * <p>
     * Default implementation which just calls {@link DataModel#removePreference(long, long)} (Object, Object)}.
     * </p>
     *
     * @throws IllegalArgumentException
     *           if userID or itemID is {@code null}
     */
    @Override
    public void removePreference(final long userID, final long itemID) {
        log.debug("Remove preference for user '{}', item '{}'", userID, itemID);
        dataModel.removePreference(userID, itemID);
    }

    @Override
    public DataModel getDataModel() {
        return dataModel;
    }

    /**
     * @param userID
     *          ID of user being evaluated
     * @param preferencesFromUser
     *          the preferences from the user
     * @return all items in the {@link DataModel} for which the user has not expressed a preference and could
     *         possibly be recommended to the user
     */
    protected FastIDSet getAllOtherItems(final long userID,
            final PreferenceArray preferencesFromUser) {
        return candidateItemsStrategy.getCandidateItems(userID,
                preferencesFromUser, dataModel);
    }

}
