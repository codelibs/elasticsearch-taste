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
package org.codelibs.elasticsearch.taste.similarity;

import java.util.Collection;

import org.codelibs.elasticsearch.taste.common.FastIDSet;
import org.codelibs.elasticsearch.taste.common.RefreshHelper;
import org.codelibs.elasticsearch.taste.common.Refreshable;
import org.codelibs.elasticsearch.taste.model.DataModel;

/**
 * Implementation of City Block distance (also known as Manhattan distance) - the absolute value of the difference of
 * each direction is summed.  The resulting unbounded distance is then mapped between 0 and 1.
 */
public final class CityBlockSimilarity extends AbstractItemSimilarity implements
        UserSimilarity {

    public CityBlockSimilarity(final DataModel dataModel) {
        super(dataModel);
    }

    /**
     * @throws UnsupportedOperationException
     */
    @Override
    public void setPreferenceInferrer(final PreferenceInferrer inferrer) {
        throw new UnsupportedOperationException();
    }

    @Override
    public void refresh(final Collection<Refreshable> alreadyRefreshed) {
        final Collection<Refreshable> refreshed = RefreshHelper
                .buildRefreshed(alreadyRefreshed);
        RefreshHelper.maybeRefresh(refreshed, getDataModel());
    }

    @Override
    public double itemSimilarity(final long itemID1, final long itemID2) {
        final DataModel dataModel = getDataModel();
        final int preferring1 = dataModel.getNumUsersWithPreferenceFor(itemID1);
        final int preferring2 = dataModel.getNumUsersWithPreferenceFor(itemID2);
        final int intersection = dataModel.getNumUsersWithPreferenceFor(
                itemID1, itemID2);
        return doSimilarity(preferring1, preferring2, intersection);
    }

    @Override
    public double[] itemSimilarities(final long itemID1, final long[] itemID2s) {
        final DataModel dataModel = getDataModel();
        final int preferring1 = dataModel.getNumUsersWithPreferenceFor(itemID1);
        final double[] distance = new double[itemID2s.length];
        for (int i = 0; i < itemID2s.length; ++i) {
            final int preferring2 = dataModel
                    .getNumUsersWithPreferenceFor(itemID2s[i]);
            final int intersection = dataModel.getNumUsersWithPreferenceFor(
                    itemID1, itemID2s[i]);
            distance[i] = doSimilarity(preferring1, preferring2, intersection);
        }
        return distance;
    }

    @Override
    public double userSimilarity(final long userID1, final long userID2) {
        final DataModel dataModel = getDataModel();
        final FastIDSet prefs1 = dataModel.getItemIDsFromUser(userID1);
        final FastIDSet prefs2 = dataModel.getItemIDsFromUser(userID2);
        final int prefs1Size = prefs1.size();
        final int prefs2Size = prefs2.size();
        final int intersectionSize = prefs1Size < prefs2Size ? prefs2
                .intersectionSize(prefs1) : prefs1.intersectionSize(prefs2);
        return doSimilarity(prefs1Size, prefs2Size, intersectionSize);
    }

    /**
     * Calculate City Block Distance from total non-zero values and intersections and map to a similarity value.
     *
     * @param pref1        number of non-zero values in left vector
     * @param pref2        number of non-zero values in right vector
     * @param intersection number of overlapping non-zero values
     */
    private static double doSimilarity(final int pref1, final int pref2,
            final int intersection) {
        final int distance = pref1 + pref2 - 2 * intersection;
        return 1.0 / (1.0 + distance);
    }

}
