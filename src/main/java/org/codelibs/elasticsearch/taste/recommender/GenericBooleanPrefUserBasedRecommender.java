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
import org.codelibs.elasticsearch.taste.neighborhood.UserNeighborhood;
import org.codelibs.elasticsearch.taste.similarity.UserSimilarity;

/**
 * A variant on {@link GenericUserBasedRecommender} which is appropriate for use when no notion of preference
 * value exists in the data.
 */
public final class GenericBooleanPrefUserBasedRecommender extends
        GenericUserBasedRecommender {

    public GenericBooleanPrefUserBasedRecommender(final DataModel dataModel,
            final UserNeighborhood neighborhood, final UserSimilarity similarity) {
        super(dataModel, neighborhood, similarity);
    }

    /**
     * This computation is in a technical sense, wrong, since in the domain of "boolean preference users" where
     * all preference values are 1, this method should only ever return 1.0 or NaN. This isn't terribly useful
     * however since it means results can't be ranked by preference value (all are 1). So instead this returns a
     * sum of similarities to any other user in the neighborhood who has also rated the item.
     */
    @Override
    protected float doEstimatePreference(final long theUserID,
            final List<SimilarUser> theNeighborhood, final long itemID) {
        if (theNeighborhood.size() == 0) {
            return Float.NaN;
        }
        final DataModel dataModel = getDataModel();
        final UserSimilarity similarity = getSimilarity();
        float totalSimilarity = 0.0f;
        boolean foundAPref = false;
        for (final SimilarUser similarUser : theNeighborhood) {
            // See GenericItemBasedRecommender.doEstimatePreference() too
            if (similarUser.getUserID() != theUserID
                    && dataModel.getPreferenceValue(similarUser.getUserID(),
                            itemID) != null) {
                foundAPref = true;
                totalSimilarity += (float) similarity.userSimilarity(theUserID,
                        similarUser.getUserID());
            }
        }
        return foundAPref ? totalSimilarity : Float.NaN;
    }

    @Override
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
    public String toString() {
        return "GenericBooleanPrefUserBasedRecommender";
    }

}
