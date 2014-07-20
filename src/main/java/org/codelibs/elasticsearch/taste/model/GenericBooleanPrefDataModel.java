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
import java.util.Map;

import org.codelibs.elasticsearch.taste.common.FastByIDMap;
import org.codelibs.elasticsearch.taste.common.FastIDSet;
import org.codelibs.elasticsearch.taste.common.LongPrimitiveArrayIterator;
import org.codelibs.elasticsearch.taste.common.LongPrimitiveIterator;
import org.codelibs.elasticsearch.taste.common.Refreshable;
import org.codelibs.elasticsearch.taste.exception.NoSuchItemException;
import org.codelibs.elasticsearch.taste.exception.NoSuchUserException;

import com.google.common.base.Preconditions;

/**
 * <p>
 * A simple {@link DataModel} which uses given user data as its data source. This implementation
 * is mostly useful for small experiments and is not recommended for contexts where performance is important.
 * </p>
 */
public final class GenericBooleanPrefDataModel extends AbstractDataModel {

    /**
     *
     */
    private static final long serialVersionUID = 1L;

    private final long[] userIDs;

    private final FastByIDMap<FastIDSet> preferenceFromUsers;

    private final long[] itemIDs;

    private final FastByIDMap<FastIDSet> preferenceForItems;

    private final FastByIDMap<FastByIDMap<Long>> timestamps;

    /**
     * <p>
     * Creates a new {@link GenericDataModel} from the given users (and their preferences). This
     * {@link DataModel} retains all this information in memory and is effectively immutable.
     * </p>
     *
     * @param userData users to include
     */
    public GenericBooleanPrefDataModel(final FastByIDMap<FastIDSet> userData) {
        this(userData, null);
    }

    /**
     * <p>
     * Creates a new {@link GenericDataModel} from the given users (and their preferences). This
     * {@link DataModel} retains all this information in memory and is effectively immutable.
     * </p>
     *
     * @param userData users to include
     * @param timestamps optionally, provided timestamps of preferences as milliseconds since the epoch.
     *  User IDs are mapped to maps of item IDs to Long timestamps.
     */
    public GenericBooleanPrefDataModel(final FastByIDMap<FastIDSet> userData,
            final FastByIDMap<FastByIDMap<Long>> timestamps) {
        Preconditions.checkArgument(userData != null, "userData is null");

        preferenceFromUsers = userData;
        preferenceForItems = new FastByIDMap<FastIDSet>();
        FastIDSet itemIDSet = new FastIDSet();
        for (final Map.Entry<Long, FastIDSet> entry : preferenceFromUsers
                .entrySet()) {
            final long userID = entry.getKey();
            final FastIDSet itemIDs = entry.getValue();
            itemIDSet.addAll(itemIDs);
            final LongPrimitiveIterator it = itemIDs.iterator();
            while (it.hasNext()) {
                final long itemID = it.nextLong();
                FastIDSet userIDs = preferenceForItems.get(itemID);
                if (userIDs == null) {
                    userIDs = new FastIDSet(2);
                    preferenceForItems.put(itemID, userIDs);
                }
                userIDs.add(userID);
            }
        }

        itemIDs = itemIDSet.toArray();
        itemIDSet = null; // Might help GC -- this is big
        Arrays.sort(itemIDs);

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
     * Exports the simple user IDs and associated item IDs in the data model.
     *
     * @return a {@link FastByIDMap} mapping user IDs to {@link FastIDSet}s representing
     *  that user's associated items
     */
    public static FastByIDMap<FastIDSet> toDataMap(final DataModel dataModel) {
        final FastByIDMap<FastIDSet> data = new FastByIDMap<FastIDSet>(
                dataModel.getNumUsers());
        final LongPrimitiveIterator it = dataModel.getUserIDs();
        while (it.hasNext()) {
            final long userID = it.nextLong();
            data.put(userID, dataModel.getItemIDsFromUser(userID));
        }
        return data;
    }

    public static FastByIDMap<FastIDSet> toDataMap(
            final FastByIDMap<PreferenceArray> data) {
        for (final Map.Entry<Long, Object> entry : ((FastByIDMap<Object>) (FastByIDMap<?>) data)
                .entrySet()) {
            final PreferenceArray prefArray = (PreferenceArray) entry
                    .getValue();
            final int size = prefArray.length();
            final FastIDSet itemIDs = new FastIDSet(size);
            for (int i = 0; i < size; i++) {
                itemIDs.add(prefArray.getItemID(i));
            }
            entry.setValue(itemIDs);
        }
        return (FastByIDMap<FastIDSet>) (FastByIDMap<?>) data;
    }

    /**
     * This is used mostly internally to the framework, and shouldn't be relied upon otherwise.
     */
    public FastByIDMap<FastIDSet> getRawUserData() {
        return preferenceFromUsers;
    }

    /**
     * This is used mostly internally to the framework, and shouldn't be relied upon otherwise.
     */
    public FastByIDMap<FastIDSet> getRawItemData() {
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
        final FastIDSet itemIDs = preferenceFromUsers.get(userID);
        if (itemIDs == null) {
            throw new NoSuchUserException(userID);
        }
        final PreferenceArray prefArray = new BooleanUserPreferenceArray(
                itemIDs.size());
        int i = 0;
        final LongPrimitiveIterator it = itemIDs.iterator();
        while (it.hasNext()) {
            prefArray.setUserID(i, userID);
            prefArray.setItemID(i, it.nextLong());
            i++;
        }
        return prefArray;
    }

    @Override
    public FastIDSet getItemIDsFromUser(final long userID) {
        final FastIDSet itemIDs = preferenceFromUsers.get(userID);
        if (itemIDs == null) {
            throw new NoSuchUserException(userID);
        }
        return itemIDs;
    }

    @Override
    public LongPrimitiveArrayIterator getItemIDs() {
        return new LongPrimitiveArrayIterator(itemIDs);
    }

    @Override
    public PreferenceArray getPreferencesForItem(final long itemID)
            throws NoSuchItemException {
        final FastIDSet userIDs = preferenceForItems.get(itemID);
        if (userIDs == null) {
            throw new NoSuchItemException(itemID);
        }
        final PreferenceArray prefArray = new BooleanItemPreferenceArray(
                userIDs.size());
        int i = 0;
        final LongPrimitiveIterator it = userIDs.iterator();
        while (it.hasNext()) {
            prefArray.setUserID(i, it.nextLong());
            prefArray.setItemID(i, itemID);
            i++;
        }
        return prefArray;
    }

    @Override
    public Float getPreferenceValue(final long userID, final long itemID)
            throws NoSuchUserException {
        final FastIDSet itemIDs = preferenceFromUsers.get(userID);
        if (itemIDs == null) {
            throw new NoSuchUserException(userID);
        }
        if (itemIDs.contains(itemID)) {
            return 1.0f;
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
        final FastIDSet userIDs1 = preferenceForItems.get(itemID);
        return userIDs1 == null ? 0 : userIDs1.size();
    }

    @Override
    public int getNumUsersWithPreferenceFor(final long itemID1,
            final long itemID2) {
        final FastIDSet userIDs1 = preferenceForItems.get(itemID1);
        if (userIDs1 == null) {
            return 0;
        }
        final FastIDSet userIDs2 = preferenceForItems.get(itemID2);
        if (userIDs2 == null) {
            return 0;
        }
        return userIDs1.size() < userIDs2.size() ? userIDs2
                .intersectionSize(userIDs1) : userIDs1
                .intersectionSize(userIDs2);
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
        return false;
    }

    @Override
    public String toString() {
        final StringBuilder result = new StringBuilder(200);
        result.append("GenericBooleanPrefDataModel[users:");
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
