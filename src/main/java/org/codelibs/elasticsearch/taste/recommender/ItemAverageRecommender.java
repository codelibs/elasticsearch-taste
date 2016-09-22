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
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;

import org.codelibs.elasticsearch.taste.common.FastByIDMap;
import org.codelibs.elasticsearch.taste.common.FastIDSet;
import org.codelibs.elasticsearch.taste.common.FullRunningAverage;
import org.codelibs.elasticsearch.taste.common.LongPrimitiveIterator;
import org.codelibs.elasticsearch.taste.common.RefreshHelper;
import org.codelibs.elasticsearch.taste.common.Refreshable;
import org.codelibs.elasticsearch.taste.common.RunningAverage;
import org.codelibs.elasticsearch.taste.exception.NoSuchUserException;
import org.codelibs.elasticsearch.taste.model.DataModel;
import org.codelibs.elasticsearch.taste.model.PreferenceArray;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.base.Preconditions;

/**
 * <p>
 * A simple recommender that always estimates preference for an item to be the average of all known preference
 * values for that item. No information about users is taken into account. This implementation is provided for
 * experimentation; while simple and fast, it may not produce very good recommendations.
 * </p>
 */
public final class ItemAverageRecommender extends AbstractRecommender {

    private static final Logger log = LoggerFactory
            .getLogger(ItemAverageRecommender.class);

    private final FastByIDMap<RunningAverage> itemAverages;

    private final ReadWriteLock buildAveragesLock;

    private final RefreshHelper refreshHelper;

    public ItemAverageRecommender(final DataModel dataModel) {
        super(dataModel);
        itemAverages = new FastByIDMap<>();
        buildAveragesLock = new ReentrantReadWriteLock();
        refreshHelper = new RefreshHelper(() -> {
            buildAverageDiffs();
            return null;
        });
        refreshHelper.addDependency(dataModel);
        buildAverageDiffs();
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

        final TopItems.Estimator<Long> estimator = new Estimator();

        final List<RecommendedItem> topItems = TopItems.getTopItems(howMany,
                possibleItemIDs.iterator(), rescorer, estimator);

        log.debug("Recommendations are: {}", topItems);
        return topItems;
    }

    @Override
    public float estimatePreference(final long userID, final long itemID) {
        final DataModel dataModel = getDataModel();
        final Float actualPref = dataModel.getPreferenceValue(userID, itemID);
        if (actualPref != null) {
            return actualPref;
        }
        return doEstimatePreference(itemID);
    }

    private float doEstimatePreference(final long itemID) {
        buildAveragesLock.readLock().lock();
        try {
            final RunningAverage average = itemAverages.get(itemID);
            return average == null ? Float.NaN : (float) average.getAverage();
        } finally {
            buildAveragesLock.readLock().unlock();
        }
    }

    private void buildAverageDiffs() {
        try {
            buildAveragesLock.writeLock().lock();
            final DataModel dataModel = getDataModel();
            final LongPrimitiveIterator it = dataModel.getUserIDs();
            while (it.hasNext()) {
                final PreferenceArray prefs = dataModel
                        .getPreferencesFromUser(it.nextLong());
                final int size = prefs.length();
                for (int i = 0; i < size; i++) {
                    final long itemID = prefs.getItemID(i);
                    RunningAverage average = itemAverages.get(itemID);
                    if (average == null) {
                        average = new FullRunningAverage();
                        itemAverages.put(itemID, average);
                    }
                    average.addDatum(prefs.getValue(i));
                }
            }
        } finally {
            buildAveragesLock.writeLock().unlock();
        }
    }

    @Override
    public void setPreference(final long userID, final long itemID,
            final float value) {
        final DataModel dataModel = getDataModel();
        double prefDelta;
        try {
            final Float oldPref = dataModel.getPreferenceValue(userID, itemID);
            prefDelta = oldPref == null ? value : value - oldPref;
        } catch (final NoSuchUserException nsee) {
            prefDelta = value;
        }
        super.setPreference(userID, itemID, value);
        try {
            buildAveragesLock.writeLock().lock();
            final RunningAverage average = itemAverages.get(itemID);
            if (average == null) {
                final RunningAverage newAverage = new FullRunningAverage();
                newAverage.addDatum(prefDelta);
                itemAverages.put(itemID, newAverage);
            } else {
                average.changeDatum(prefDelta);
            }
        } finally {
            buildAveragesLock.writeLock().unlock();
        }
    }

    @Override
    public void removePreference(final long userID, final long itemID) {
        final DataModel dataModel = getDataModel();
        final Float oldPref = dataModel.getPreferenceValue(userID, itemID);
        super.removePreference(userID, itemID);
        if (oldPref != null) {
            try {
                buildAveragesLock.writeLock().lock();
                final RunningAverage average = itemAverages.get(itemID);
                if (average == null) {
                    throw new IllegalStateException(
                            "No preferences exist for item ID: " + itemID);
                } else {
                    average.removeDatum(oldPref);
                }
            } finally {
                buildAveragesLock.writeLock().unlock();
            }
        }
    }

    @Override
    public void refresh(final Collection<Refreshable> alreadyRefreshed) {
        refreshHelper.refresh(alreadyRefreshed);
    }

    @Override
    public String toString() {
        return "ItemAverageRecommender";
    }

    private final class Estimator implements TopItems.Estimator<Long> {

        @Override
        public double estimate(final Long itemID) {
            return doEstimatePreference(itemID);
        }
    }

}
