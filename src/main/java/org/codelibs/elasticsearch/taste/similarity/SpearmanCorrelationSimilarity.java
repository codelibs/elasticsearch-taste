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

import org.codelibs.elasticsearch.taste.common.RefreshHelper;
import org.codelibs.elasticsearch.taste.common.Refreshable;
import org.codelibs.elasticsearch.taste.model.DataModel;
import org.codelibs.elasticsearch.taste.model.PreferenceArray;

import com.google.common.base.Preconditions;

/**
 * <p>
 * Like {@link PearsonCorrelationSimilarity}, but compares relative ranking of preference values instead of
 * preference values themselves. That is, each user's preferences are sorted and then assign a rank as their
 * preference value, with 1 being assigned to the least preferred item.
 * </p>
 */
public final class SpearmanCorrelationSimilarity implements UserSimilarity {

    private final DataModel dataModel;

    public SpearmanCorrelationSimilarity(final DataModel dataModel) {
        this.dataModel = Preconditions.checkNotNull(dataModel);
    }

    @Override
    public double userSimilarity(final long userID1, final long userID2) {
        PreferenceArray xPrefs = dataModel.getPreferencesFromUser(userID1);
        PreferenceArray yPrefs = dataModel.getPreferencesFromUser(userID2);
        final int xLength = xPrefs.length();
        final int yLength = yPrefs.length();

        if (xLength <= 1 || yLength <= 1) {
            return Double.NaN;
        }

        // Copy prefs since we need to modify pref values to ranks
        xPrefs = xPrefs.clone();
        yPrefs = yPrefs.clone();

        // First sort by values from low to high
        xPrefs.sortByValue();
        yPrefs.sortByValue();

        // Assign ranks from low to high
        float nextRank = 1.0f;
        for (int i = 0; i < xLength; i++) {
            // ... but only for items that are common to both pref arrays
            if (yPrefs.hasPrefWithItemID(xPrefs.getItemID(i))) {
                xPrefs.setValue(i, nextRank);
                nextRank += 1.0f;
            }
            // Other values are bogus but don't matter
        }
        nextRank = 1.0f;
        for (int i = 0; i < yLength; i++) {
            if (xPrefs.hasPrefWithItemID(yPrefs.getItemID(i))) {
                yPrefs.setValue(i, nextRank);
                nextRank += 1.0f;
            }
        }

        xPrefs.sortByItem();
        yPrefs.sortByItem();

        long xIndex = xPrefs.getItemID(0);
        long yIndex = yPrefs.getItemID(0);
        int xPrefIndex = 0;
        int yPrefIndex = 0;

        double sumXYRankDiff2 = 0.0;
        int count = 0;

        while (true) {
            final int compare = xIndex < yIndex ? -1 : xIndex > yIndex ? 1 : 0;
            if (compare == 0) {
                final double diff = xPrefs.getValue(xPrefIndex)
                        - yPrefs.getValue(yPrefIndex);
                sumXYRankDiff2 += diff * diff;
                count++;
            }
            if (compare <= 0) {
                if (++xPrefIndex >= xLength) {
                    break;
                }
                xIndex = xPrefs.getItemID(xPrefIndex);
            }
            if (compare >= 0) {
                if (++yPrefIndex >= yLength) {
                    break;
                }
                yIndex = yPrefs.getItemID(yPrefIndex);
            }
        }

        if (count <= 1) {
            return Double.NaN;
        }

        // When ranks are unique, this formula actually gives the Pearson correlation
        return 1.0 - 6.0 * sumXYRankDiff2 / (count * (count * count - 1));
    }

    @Override
    public void setPreferenceInferrer(final PreferenceInferrer inferrer) {
        throw new UnsupportedOperationException();
    }

    @Override
    public void refresh(Collection<Refreshable> alreadyRefreshed) {
        alreadyRefreshed = RefreshHelper.buildRefreshed(alreadyRefreshed);
        RefreshHelper.maybeRefresh(alreadyRefreshed, dataModel);
    }

}
