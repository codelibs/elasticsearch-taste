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

import java.util.Collection;

import org.codelibs.elasticsearch.taste.common.FastIDSet;
import org.codelibs.elasticsearch.taste.common.LongPrimitiveIterator;
import org.codelibs.elasticsearch.taste.common.Refreshable;
import org.codelibs.elasticsearch.taste.exception.NoSuchItemException;
import org.codelibs.elasticsearch.taste.exception.NoSuchUserException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.base.Preconditions;

/**
 * <p>
 * This {@link DataModel} decorator class is useful in a situation where you wish to recommend to a user that
 * doesn't really exist yet in your actual {@link DataModel}. For example maybe you wish to recommend DVDs to
 * a user who has browsed a few titles on your DVD store site, but, the user is not yet registered.
 * </p>
 *
 * <p>
 * This enables you to temporarily add a temporary user to an existing {@link DataModel} in a way that
 * recommenders can then produce recommendations anyway. To do so, wrap your real implementation in this
 * class:
 * </p>
 *
 * <p>
 *
 * <pre>
 * DataModel realModel = ...;
 * DataModel plusModel = new PlusAnonymousUserDataModel(realModel);
 * ...
 * ItemSimilarity similarity = new LogLikelihoodSimilarity(realModel); // not plusModel
 * </pre>
 *
 * </p>
 *
 * <p>
 * But, you may continue to use {@code realModel} as input to other components. To recommend, first construct and
 * set the temporary user information on the model and then simply call the recommender. The
 * {@code synchronized} block exists to remind you that this is of course not thread-safe. Only one set
 * of temp data can be inserted into the model and used at one time.
 * </p>
 *
 * <p>
 *
 * <pre>
 * Recommender recommender = ...;
 * ...
 * synchronized(...) {
 *   PreferenceArray tempPrefs = ...;
 *   plusModel.setTempPrefs(tempPrefs);
 *   recommender.recommend(PlusAnonymousUserDataModel.TEMP_USER_ID, 10);
 *   plusModel.setTempPrefs(null);
 * }
 * </pre>
 *
 * </p>
 */
public class PlusAnonymousUserDataModel implements DataModel {

    /**
     *
     */
    private static final long serialVersionUID = 1L;

    public static final long TEMP_USER_ID = Long.MIN_VALUE;

    private final DataModel delegate;

    private PreferenceArray tempPrefs;

    private final FastIDSet prefItemIDs;

    private static final Logger log = LoggerFactory
            .getLogger(PlusAnonymousUserDataModel.class);

    public PlusAnonymousUserDataModel(final DataModel delegate) {
        this.delegate = delegate;
        prefItemIDs = new FastIDSet();
    }

    protected DataModel getDelegate() {
        return delegate;
    }

    public void setTempPrefs(final PreferenceArray prefs) {
        Preconditions.checkArgument(prefs != null && prefs.length() > 0,
                "prefs is null or empty");
        tempPrefs = prefs;
        prefItemIDs.clear();
        for (int i = 0; i < prefs.length(); i++) {
            prefItemIDs.add(prefs.getItemID(i));
        }
    }

    public void clearTempPrefs() {
        tempPrefs = null;
        prefItemIDs.clear();
    }

    @Override
    public LongPrimitiveIterator getUserIDs() {
        if (tempPrefs == null) {
            return delegate.getUserIDs();
        }
        return new PlusAnonymousUserLongPrimitiveIterator(
                delegate.getUserIDs(), TEMP_USER_ID);
    }

    @Override
    public PreferenceArray getPreferencesFromUser(final long userID) {
        if (userID == TEMP_USER_ID) {
            if (tempPrefs == null) {
                throw new NoSuchUserException(TEMP_USER_ID);
            }
            return tempPrefs;
        }
        return delegate.getPreferencesFromUser(userID);
    }

    @Override
    public FastIDSet getItemIDsFromUser(final long userID) {
        if (userID == TEMP_USER_ID) {
            if (tempPrefs == null) {
                throw new NoSuchUserException(TEMP_USER_ID);
            }
            return prefItemIDs;
        }
        return delegate.getItemIDsFromUser(userID);
    }

    @Override
    public LongPrimitiveIterator getItemIDs() {
        return delegate.getItemIDs();
        // Yeah ignoring items that only the plus-one user knows about... can't really happen
    }

    @Override
    public PreferenceArray getPreferencesForItem(final long itemID) {
        if (tempPrefs == null) {
            return delegate.getPreferencesForItem(itemID);
        }
        PreferenceArray delegatePrefs = null;
        try {
            delegatePrefs = delegate.getPreferencesForItem(itemID);
        } catch (final NoSuchItemException nsie) {
            // OK. Probably an item that only the anonymous user has
            if (log.isDebugEnabled()) {
                log.debug("Item {} unknown", itemID);
            }
        }
        for (int i = 0; i < tempPrefs.length(); i++) {
            if (tempPrefs.getItemID(i) == itemID) {
                return cloneAndMergeInto(delegatePrefs, itemID,
                        tempPrefs.getUserID(i), tempPrefs.getValue(i));
            }
        }
        if (delegatePrefs == null) {
            // No, didn't find it among the anonymous user prefs
            throw new NoSuchItemException(itemID);
        }
        return delegatePrefs;
    }

    private static PreferenceArray cloneAndMergeInto(
            final PreferenceArray delegatePrefs, final long itemID,
            final long newUserID, final float value) {

        final int length = delegatePrefs == null ? 0 : delegatePrefs.length();
        final int newLength = length + 1;
        final PreferenceArray newPreferenceArray = new GenericItemPreferenceArray(
                newLength);

        // Set item ID once
        newPreferenceArray.setItemID(0, itemID);

        int positionToInsert = 0;
        while (positionToInsert < length
                && newUserID > delegatePrefs.getUserID(positionToInsert)) {
            positionToInsert++;
        }

        for (int i = 0; i < positionToInsert; i++) {
            newPreferenceArray.setUserID(i, delegatePrefs.getUserID(i));
            newPreferenceArray.setValue(i, delegatePrefs.getValue(i));
        }
        newPreferenceArray.setUserID(positionToInsert, newUserID);
        newPreferenceArray.setValue(positionToInsert, value);
        for (int i = positionToInsert + 1; i < newLength; i++) {
            newPreferenceArray.setUserID(i, delegatePrefs.getUserID(i - 1));
            newPreferenceArray.setValue(i, delegatePrefs.getValue(i - 1));
        }

        return newPreferenceArray;
    }

    @Override
    public Float getPreferenceValue(final long userID, final long itemID) {
        if (userID == TEMP_USER_ID) {
            if (tempPrefs == null) {
                throw new NoSuchUserException(TEMP_USER_ID);
            }
            for (int i = 0; i < tempPrefs.length(); i++) {
                if (tempPrefs.getItemID(i) == itemID) {
                    return tempPrefs.getValue(i);
                }
            }
            return null;
        }
        return delegate.getPreferenceValue(userID, itemID);
    }

    @Override
    public Long getPreferenceTime(final long userID, final long itemID) {
        if (userID == TEMP_USER_ID) {
            if (tempPrefs == null) {
                throw new NoSuchUserException(TEMP_USER_ID);
            }
            return null;
        }
        return delegate.getPreferenceTime(userID, itemID);
    }

    @Override
    public int getNumItems() {
        return delegate.getNumItems();
    }

    @Override
    public int getNumUsers() {
        return delegate.getNumUsers() + (tempPrefs == null ? 0 : 1);
    }

    @Override
    public int getNumUsersWithPreferenceFor(final long itemID) {
        if (tempPrefs == null) {
            return delegate.getNumUsersWithPreferenceFor(itemID);
        }
        boolean found = false;
        for (int i = 0; i < tempPrefs.length(); i++) {
            if (tempPrefs.getItemID(i) == itemID) {
                found = true;
                break;
            }
        }
        return delegate.getNumUsersWithPreferenceFor(itemID) + (found ? 1 : 0);
    }

    @Override
    public int getNumUsersWithPreferenceFor(final long itemID1,
            final long itemID2) {
        if (tempPrefs == null) {
            return delegate.getNumUsersWithPreferenceFor(itemID1, itemID2);
        }
        boolean found1 = false;
        boolean found2 = false;
        for (int i = 0; i < tempPrefs.length() && !(found1 && found2); i++) {
            final long itemID = tempPrefs.getItemID(i);
            if (itemID == itemID1) {
                found1 = true;
            }
            if (itemID == itemID2) {
                found2 = true;
            }
        }
        return delegate.getNumUsersWithPreferenceFor(itemID1, itemID2)
                + (found1 && found2 ? 1 : 0);
    }

    @Override
    public void setPreference(final long userID, final long itemID,
            final float value) {
        if (userID == TEMP_USER_ID) {
            if (tempPrefs == null) {
                throw new NoSuchUserException(TEMP_USER_ID);
            }
            throw new UnsupportedOperationException();
        }
        delegate.setPreference(userID, itemID, value);
    }

    @Override
    public void removePreference(final long userID, final long itemID) {
        if (userID == TEMP_USER_ID) {
            if (tempPrefs == null) {
                throw new NoSuchUserException(TEMP_USER_ID);
            }
            throw new UnsupportedOperationException();
        }
        delegate.removePreference(userID, itemID);
    }

    @Override
    public void refresh(final Collection<Refreshable> alreadyRefreshed) {
        delegate.refresh(alreadyRefreshed);
    }

    @Override
    public boolean hasPreferenceValues() {
        return delegate.hasPreferenceValues();
    }

    @Override
    public float getMaxPreference() {
        return delegate.getMaxPreference();
    }

    @Override
    public float getMinPreference() {
        return delegate.getMinPreference();
    }

}
