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

package org.codelibs.elasticsearch.taste.neighborhood;

import java.util.ArrayList;
import java.util.List;

import org.codelibs.elasticsearch.taste.common.LongPrimitiveIterator;
import org.codelibs.elasticsearch.taste.common.SamplingLongPrimitiveIterator;
import org.codelibs.elasticsearch.taste.model.DataModel;
import org.codelibs.elasticsearch.taste.recommender.SimilarUser;
import org.codelibs.elasticsearch.taste.similarity.UserSimilarity;

import com.google.common.base.Preconditions;

/**
 * <p>
 * Computes a neigbhorhood consisting of all users whose similarity to the given user meets or exceeds a
 * certain threshold. Similarity is defined by the given {@link UserSimilarity}.
 * </p>
 */
public final class ThresholdUserNeighborhood extends AbstractUserNeighborhood {

    private final double threshold;

    /**
     * @param threshold
     *          similarity threshold
     * @param userSimilarity
     *          similarity metric
     * @param dataModel
     *          data model
     * @throws IllegalArgumentException
     *           if threshold is {@link Double#NaN}, or if samplingRate is not positive and less than or equal
     *           to 1.0, or if userSimilarity or dataModel are {@code null}
     */
    public ThresholdUserNeighborhood(final double threshold,
            final UserSimilarity userSimilarity, final DataModel dataModel) {
        this(threshold, userSimilarity, dataModel, 1.0);
    }

    /**
     * @param threshold
     *          similarity threshold
     * @param userSimilarity
     *          similarity metric
     * @param dataModel
     *          data model
     * @param samplingRate
     *          percentage of users to consider when building neighborhood -- decrease to trade quality for
     *          performance
     * @throws IllegalArgumentException
     *           if threshold or samplingRate is {@link Double#NaN}, or if samplingRate is not positive and less
     *           than or equal to 1.0, or if userSimilarity or dataModel are {@code null}
     */
    public ThresholdUserNeighborhood(final double threshold,
            final UserSimilarity userSimilarity, final DataModel dataModel,
            final double samplingRate) {
        super(userSimilarity, dataModel, samplingRate);
        Preconditions.checkArgument(!Double.isNaN(threshold),
                "threshold must not be NaN");
        this.threshold = threshold;
    }

    @Override
    public List<SimilarUser> getUserNeighborhood(final long userID) {

        final DataModel dataModel = getDataModel();
        final List<SimilarUser> neighborhood = new ArrayList<>();
        final LongPrimitiveIterator usersIterable = SamplingLongPrimitiveIterator
                .maybeWrapIterator(dataModel.getUserIDs(), getSamplingRate());
        final UserSimilarity userSimilarityImpl = getUserSimilarity();

        while (usersIterable.hasNext()) {
            final long otherUserID = usersIterable.next();
            if (userID != otherUserID) {
                final double theSimilarity = userSimilarityImpl.userSimilarity(
                        userID, otherUserID);
                if (!Double.isNaN(theSimilarity) && theSimilarity >= threshold) {
                    neighborhood
                            .add(new SimilarUser(otherUserID, theSimilarity));
                }
            }
        }

        return neighborhood;
    }

    @Override
    public String toString() {
        return "ThresholdUserNeighborhood";
    }

}
