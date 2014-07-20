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

import java.io.Serializable;

import org.codelibs.elasticsearch.taste.common.FastIDSet;
import org.codelibs.elasticsearch.taste.common.LongPrimitiveIterator;
import org.codelibs.elasticsearch.taste.common.Refreshable;

/**
 * <p>
 * Implementations represent a repository of information about users and their associated {@link Preference}s
 * for items.
 * </p>
 */
public interface DataModel extends Refreshable, Serializable {

    /**
     * @return all user IDs in the model, in order
     */
    LongPrimitiveIterator getUserIDs();

    /**
     * @param userID
     *          ID of user to get prefs for
     * @return user's preferences, ordered by item ID
     * @throws org.codelibs.elasticsearch.taste.exception.NoSuchUserException
     *           if the user does not exist
     */
    PreferenceArray getPreferencesFromUser(long userID);

    /**
     * @param userID
     *          ID of user to get prefs for
     * @return IDs of items user expresses a preference for
     * @throws org.codelibs.elasticsearch.taste.exception.NoSuchUserException
     *           if the user does not exist
     */
    FastIDSet getItemIDsFromUser(long userID);

    /**
     * @return a {@link LongPrimitiveIterator} of all item IDs in the model, in order
     */
    LongPrimitiveIterator getItemIDs();

    /**
     * @param itemID
     *          item ID
     * @return all existing {@link Preference}s expressed for that item, ordered by user ID, as an array
     * @throws org.codelibs.elasticsearch.taste.exception.NoSuchItemException
     *           if the item does not exist
     */
    PreferenceArray getPreferencesForItem(long itemID);

    /**
     * Retrieves the preference value for a single user and item.
     *
     * @param userID
     *          user ID to get pref value from
     * @param itemID
     *          item ID to get pref value for
     * @return preference value from the given user for the given item or null if none exists
     * @throws org.codelibs.elasticsearch.taste.exception.NoSuchUserException
     *           if the user does not exist
     */
    Float getPreferenceValue(long userID, long itemID);

    /**
     * Retrieves the time at which a preference value from a user and item was set, if known.
     * Time is expressed in the usual way, as a number of milliseconds since the epoch.
     *
     * @param userID user ID for preference in question
     * @param itemID item ID for preference in question
     * @return time at which preference was set or null if no preference exists or its time is not known
     * @throws org.codelibs.elasticsearch.taste.exception.NoSuchUserException if the user does not exist
     */
    Long getPreferenceTime(long userID, long itemID);

    /**
     * @return total number of items known to the model. This is generally the union of all items preferred by
     *         at least one user but could include more.
     */
    int getNumItems();

    /**
     * @return total number of users known to the model.
     */
    int getNumUsers();

    /**
     * @param itemID item ID to check for
     * @return the number of users who have expressed a preference for the item
     */
    int getNumUsersWithPreferenceFor(long itemID);

    /**
     * @param itemID1 first item ID to check for
     * @param itemID2 second item ID to check for
     * @return the number of users who have expressed a preference for the items
     */
    int getNumUsersWithPreferenceFor(long itemID1, long itemID2);

    /**
     * <p>
     * Sets a particular preference (item plus rating) for a user.
     * </p>
     *
     * @param userID
     *          user to set preference for
     * @param itemID
     *          item to set preference for
     * @param value
     *          preference value
     * @throws org.codelibs.elasticsearch.taste.exception.NoSuchItemException
     *           if the item does not exist
     * @throws org.codelibs.elasticsearch.taste.exception.NoSuchUserException
     *           if the user does not exist
     */
    void setPreference(long userID, long itemID, float value);

    /**
     * <p>
     * Removes a particular preference for a user.
     * </p>
     *
     * @param userID
     *          user from which to remove preference
     * @param itemID
     *          item to remove preference for
     * @throws org.codelibs.elasticsearch.taste.exception.NoSuchItemException
     *           if the item does not exist
     * @throws org.codelibs.elasticsearch.taste.exception.NoSuchUserException
     *           if the user does not exist
     */
    void removePreference(long userID, long itemID);

    /**
     * @return true if this implementation actually stores and returns distinct preference values;
     *  that is, if it is not a 'boolean' DataModel
     */
    boolean hasPreferenceValues();

    /**
     * @return the maximum preference value that is possible in the current problem domain being evaluated. For
     * example, if the domain is movie ratings on a scale of 1 to 5, this should be 5. While a
     * {@link org.codelibs.elasticsearch.taste.recommender.Recommender} may estimate a preference value above 5.0, it
     * isn't "fair" to consider that the system is actually suggesting an impossible rating of, say, 5.4 stars.
     * In practice the application would cap this estimate to 5.0. Since evaluators evaluate
     * the difference between estimated and actual value, this at least prevents this effect from unfairly
     * penalizing a {@link org.codelibs.elasticsearch.taste.recommender.Recommender}
     */
    float getMaxPreference();

    /**
     * @see #getMaxPreference()
     */
    float getMinPreference();

}
