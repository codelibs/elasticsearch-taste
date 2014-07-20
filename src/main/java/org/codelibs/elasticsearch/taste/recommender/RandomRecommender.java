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
import java.util.List;
import java.util.Random;

import org.apache.mahout.common.RandomUtils;
import org.codelibs.elasticsearch.taste.common.LongPrimitiveIterator;
import org.codelibs.elasticsearch.taste.common.Refreshable;
import org.codelibs.elasticsearch.taste.model.DataModel;
import org.codelibs.elasticsearch.taste.model.PreferenceArray;

import com.google.common.collect.Lists;

/**
 * Produces random recommendations and preference estimates. This is likely only useful as a novelty and for
 * benchmarking.
 */
public final class RandomRecommender extends AbstractRecommender {

    private final Random random = RandomUtils.getRandom();

    private final float minPref;

    private final float maxPref;

    public RandomRecommender(final DataModel dataModel) {
        super(dataModel);
        float maxPref = Float.NEGATIVE_INFINITY;
        float minPref = Float.POSITIVE_INFINITY;
        final LongPrimitiveIterator userIterator = dataModel.getUserIDs();
        while (userIterator.hasNext()) {
            final long userID = userIterator.next();
            final PreferenceArray prefs = dataModel
                    .getPreferencesFromUser(userID);
            for (int i = 0; i < prefs.length(); i++) {
                final float prefValue = prefs.getValue(i);
                if (prefValue < minPref) {
                    minPref = prefValue;
                }
                if (prefValue > maxPref) {
                    maxPref = prefValue;
                }
            }
        }
        this.minPref = minPref;
        this.maxPref = maxPref;
    }

    @Override
    public List<RecommendedItem> recommend(final long userID,
            final int howMany, final IDRescorer rescorer) {
        final DataModel dataModel = getDataModel();
        final int numItems = dataModel.getNumItems();
        final List<RecommendedItem> result = Lists
                .newArrayListWithCapacity(howMany);
        while (result.size() < howMany) {
            final LongPrimitiveIterator it = dataModel.getItemIDs();
            it.skip(random.nextInt(numItems));
            final long itemID = it.next();
            if (dataModel.getPreferenceValue(userID, itemID) == null) {
                result.add(new GenericRecommendedItem(itemID, randomPref()));
            }
        }
        return result;
    }

    @Override
    public float estimatePreference(final long userID, final long itemID) {
        return randomPref();
    }

    private float randomPref() {
        return minPref + random.nextFloat() * (maxPref - minPref);
    }

    @Override
    public void refresh(final Collection<Refreshable> alreadyRefreshed) {
        getDataModel().refresh(alreadyRefreshed);
    }

}
