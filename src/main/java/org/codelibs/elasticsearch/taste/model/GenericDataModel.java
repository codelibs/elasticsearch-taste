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

package org.codelibs.elasticsearch.taste.model;

import java.util.Arrays;
import java.util.Collection;
import java.util.List;
import java.util.Map;

import org.codelibs.elasticsearch.taste.common.FastByIDMap;
import org.codelibs.elasticsearch.taste.common.FastIDSet;
import org.codelibs.elasticsearch.taste.common.LongPrimitiveArrayIterator;
import org.codelibs.elasticsearch.taste.common.LongPrimitiveIterator;
import org.codelibs.elasticsearch.taste.common.Refreshable;
import org.codelibs.elasticsearch.taste.exception.NoSuchItemException;
import org.codelibs.elasticsearch.taste.exception.NoSuchUserException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.base.Preconditions;
import com.google.common.collect.Lists;

/**
 * <p>
 * A simple {@link DataModel} which uses a given {@link List} of users as its data source. This implementation
 * is mostly useful for small experiments and is not recommended for contexts where performance is important.
 * </p>
 */
public final class GenericDataModel extends AbstractDataModel {

    /**
     *
     */
    private static final long serialVersionUID = 1L;

    private static final Logger log = LoggerFactory
            .getLogger(GenericDataModel.class);

    private final long[] userIDs;

    private final FastByIDMap<PreferenceArray> preferenceFromUsers;

    private final long[] itemIDs;

    private final FastByIDMap<PreferenceArray> preferenceForItems;

    private final FastByIDMap<FastByIDMap<Long>> timestamps;

    /**
     * <p>
     * Creates a new {@link GenericDataModel} from the given users (and their preferences). This
     * {@link DataModel} retains all this information in memory and is effectively immutable.
     * </p>
     *
     * @param userData users to include; (see also {@link #toDataMap(FastByIDMap, boolean)})
     */
    public GenericDataModel(final FastByIDMap<PreferenceArray> userData) {
        this(userData, null);
    }

    /**
     * <p>
     * Creates a new {@link GenericDataModel} from the given users (and their preferences). This
     * {@link DataModel} retains all this information in memory and is effectively immutable.
     * </p>
     *
     * @param userData users to include; (see also {@link #toDataMap(FastByIDMap, boolean)})
     * @param timestamps optionally, provided timestamps of preferences as milliseconds since the epoch.
     *  User IDs are mapped to maps of item IDs to Long timestamps.
     */
    public GenericDataModel(final FastByIDMap<PreferenceArray> userData,
            final FastByIDMap<FastByIDMap<Long>> timestamps) {
        Preconditions.checkArgument(userData != null, "userData is null");

        preferenceFromUsers = userData;
        final FastByIDMap<Collection<Preference>> prefsForItems = new FastByIDMap<Collection<Preference>>();
        FastIDSet itemIDSet = new FastIDSet();
        int currentCount = 0;
        float maxPrefValue = Float.NEGATIVE_INFINITY;
        float minPrefValue = Float.POSITIVE_INFINITY;
        for (final Map.Entry<Long, PreferenceArray> entry : preferenceFromUsers
                .entrySet()) {
            final PreferenceArray prefs = entry.getValue();
            prefs.sortByItem();
            for (final Preference preference : prefs) {
                final long itemID = preference.getItemID();
                itemIDSet.add(itemID);
                Collection<Preference> prefsForItem = prefsForItems.get(itemID);
                if (prefsForItem == null) {
                    prefsForItem = Lists.newArrayListWithCapacity(2);
                    prefsForItems.put(itemID, prefsForItem);
                }
                prefsForItem.add(preference);
                final float value = preference.getValue();
                if (value > maxPrefValue) {
                    maxPrefValue = value;
                }
                if (value < minPrefValue) {
                    minPrefValue = value;
                }
            }
            if (++currentCount % 10000 == 0) {
                log.info("Processed {} users", currentCount);
            }
        }
        log.info("Processed {} users", currentCount);

        setMinPreference(minPrefValue);
        setMaxPreference(maxPrefValue);

        itemIDs = itemIDSet.toArray();
        itemIDSet = null; // Might help GC -- this is big
        Arrays.sort(itemIDs);

        preferenceForItems = toDataMap(prefsForItems, false);

        for (final Map.Entry<Long, PreferenceArray> entry : preferenceForItems
                .entrySet()) {
            entry.getValue().sortByUser();
        }

        userIDs = new long[userData.size()];
        int i = 0;
        final LongPrimitiveIterator it = userData.keySetIterator();
        while (it.hasNext()) {
            userIDs[i++] = it.next();
        }
        Arrays.sort(userIDs);

        this.timestamps = timestamps;
    }

 

    /**
     * Swaps, in-place, {@link List}s for arrays in {@link Map} values .
     *
     * @return input value
     */
    public static FastByIDMap<PreferenceArray> toDataMap(
            final FastByIDMap<Collection<Preference>> data, final boolean byUser) {
        for (final Map.Entry<Long, Object> entry : ((FastByIDMap<Object>) (FastByIDMap<?>) data)
                .entrySet()) {
            final List<Preference> prefList = (List<Preference>) entry
                    .getValue();
            entry.setValue(byUser ? new GenericUserPreferenceArray(prefList)
                    : new GenericItemPreferenceArray(prefList));
        }
        return (FastByIDMap<PreferenceArray>) (FastByIDMap<?>) data;
    }

    /**
     * Exports the simple user IDs and preferences in the data model.
     *
     * @return a {@link FastByIDMap} mapping user IDs to {@link PreferenceArray}s representing
     *  that user's preferences
     */
    public static FastByIDMap<PreferenceArray> toDataMap(
            final DataModel dataModel) {
        final FastByIDMap<PreferenceArray> data = new FastByIDMap<PreferenceArray>(
                dataModel.getNumUsers());
        final LongPrimitiveIterator it = dataModel.getUserIDs();
        while (it.hasNext()) {
            final long userID = it.nextLong();
            data.put(userID, dataModel.getPreferencesFromUser(userID));
        }
        return data;
    }

    /**
     * This is used mostly internally to the framework, and shouldn't be relied upon otherwise.
     */
    public FastByIDMap<PreferenceArray> getRawUserData() {
        return preferenceFromUsers;
    }

    /**
     * This is used mostly internally to the framework, and shouldn't be relied upon otherwise.
     */
    public FastByIDMap<PreferenceArray> getRawItemData() {
        return preferenceForItems;
    }

    @Override
    public LongPrimitiveArrayIterator getUserIDs() {
        return new LongPrimitiveArrayIterator(userIDs);
    }

    /**
     * @throws NoSuchUserException
     *           if there is no such user
     */
    @Override
    public PreferenceArray getPreferencesFromUser(final long userID)
            throws NoSuchUserException {
        final PreferenceArray prefs = preferenceFromUsers.get(userID);
        if (prefs == null) {
            throw new NoSuchUserException(userID);
        }
        return prefs;
    }

    @Override
    public FastIDSet getItemIDsFromUser(final long userID) {
        final PreferenceArray prefs = getPreferencesFromUser(userID);
        final int size = prefs.length();
        final FastIDSet result = new FastIDSet(size);
        for (int i = 0; i < size; i++) {
            result.add(prefs.getItemID(i));
        }
        return result;
    }

    @Override
    public LongPrimitiveArrayIterator getItemIDs() {
        return new LongPrimitiveArrayIterator(itemIDs);
    }

    @Override
    public PreferenceArray getPreferencesForItem(final long itemID)
            throws NoSuchItemException {
        final PreferenceArray prefs = preferenceForItems.get(itemID);
        if (prefs == null) {
            throw new NoSuchItemException(itemID);
        }
        return prefs;
    }

    @Override
    public Float getPreferenceValue(final long userID, final long itemID) {
        final PreferenceArray prefs = getPreferencesFromUser(userID);
        final int size = prefs.length();
        for (int i = 0; i < size; i++) {
            if (prefs.getItemID(i) == itemID) {
                return prefs.getValue(i);
            }
        }
        return null;
    }

    @Override
    public Long getPreferenceTime(final long userID, final long itemID) {
        if (timestamps == null) {
            return null;
        }
        final FastByIDMap<Long> itemTimestamps = timestamps.get(userID);
        if (itemTimestamps == null) {
            throw new NoSuchUserException(userID);
        }
        return itemTimestamps.get(itemID);
    }

    @Override
    public int getNumItems() {
        return itemIDs.length;
    }

    @Override
    public int getNumUsers() {
        return userIDs.length;
    }

    @Override
    public int getNumUsersWithPreferenceFor(final long itemID) {
        final PreferenceArray prefs1 = preferenceForItems.get(itemID);
        return prefs1 == null ? 0 : prefs1.length();
    }

    @Override
    public int getNumUsersWithPreferenceFor(final long itemID1,
            final long itemID2) {
        final PreferenceArray prefs1 = preferenceForItems.get(itemID1);
        if (prefs1 == null) {
            return 0;
        }
        final PreferenceArray prefs2 = preferenceForItems.get(itemID2);
        if (prefs2 == null) {
            return 0;
        }

        final int size1 = prefs1.length();
        final int size2 = prefs2.length();
        int count = 0;
        int i = 0;
        int j = 0;
        long userID1 = prefs1.getUserID(0);
        long userID2 = prefs2.getUserID(0);
        while (true) {
            if (userID1 < userID2) {
                if (++i == size1) {
                    break;
                }
                userID1 = prefs1.getUserID(i);
            } else if (userID1 > userID2) {
                if (++j == size2) {
                    break;
                }
                userID2 = prefs2.getUserID(j);
            } else {
                count++;
                if (++i == size1 || ++j == size2) {
                    break;
                }
                userID1 = prefs1.getUserID(i);
                userID2 = prefs2.getUserID(j);
            }
        }
        return count;
    }

    @Override
    public void removePreference(final long userID, final long itemID) {
        throw new UnsupportedOperationException();
    }

    @Override
    public void setPreference(final long userID, final long itemID,
            final float value) {
        throw new UnsupportedOperationException();
    }

    @Override
    public void refresh(final Collection<Refreshable> alreadyRefreshed) {
        // Does nothing
    }

    @Override
    public boolean hasPreferenceValues() {
        return true;
    }

    @Override
    public String toString() {
        final StringBuilder result = new StringBuilder(200);
        result.append("GenericDataModel[users:");
        for (int i = 0; i < Math.min(3, userIDs.length); i++) {
            if (i > 0) {
                result.append(',');
            }
            result.append(userIDs[i]);
        }
        if (userIDs.length > 3) {
            result.append("...");
        }
        result.append(']');
        return result.toString();
    }

}
