/*
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

package org.codelibs.elasticsearch.taste.eval;

import java.util.Iterator;
import java.util.List;

import org.codelibs.elasticsearch.taste.common.FastByIDMap;
import org.codelibs.elasticsearch.taste.common.FastIDSet;
import org.codelibs.elasticsearch.taste.model.DataModel;
import org.codelibs.elasticsearch.taste.model.GenericUserPreferenceArray;
import org.codelibs.elasticsearch.taste.model.Preference;
import org.codelibs.elasticsearch.taste.model.PreferenceArray;

import com.google.common.collect.Lists;

/**
 * Picks relevant items to be those with the strongest preference, and
 * includes the other users' preferences in full.
 */
public final class GenericRelevantItemsDataSplitter implements
        RelevantItemsDataSplitter {

    @Override
    public FastIDSet getRelevantItemsIDs(final long userID, final int at,
            final double relevanceThreshold, final DataModel dataModel) {
        final PreferenceArray prefs = dataModel.getPreferencesFromUser(userID);
        final FastIDSet relevantItemIDs = new FastIDSet(at);
        prefs.sortByValueReversed();
        for (int i = 0; i < prefs.length() && relevantItemIDs.size() < at; i++) {
            if (prefs.getValue(i) >= relevanceThreshold) {
                relevantItemIDs.add(prefs.getItemID(i));
            }
        }
        return relevantItemIDs;
    }

    @Override
    public void processOtherUser(final long userID,
            final FastIDSet relevantItemIDs,
            final FastByIDMap<PreferenceArray> trainingUsers,
            final long otherUserID, final DataModel dataModel) {
        final PreferenceArray prefs2Array = dataModel
                .getPreferencesFromUser(otherUserID);
        // If we're dealing with the very user that we're evaluating for precision/recall,
        if (userID == otherUserID) {
            // then must remove all the test IDs, the "relevant" item IDs
            final List<Preference> prefs2 = Lists
                    .newArrayListWithCapacity(prefs2Array.length());
            for (final Preference pref : prefs2Array) {
                prefs2.add(pref);
            }
            for (final Iterator<Preference> iterator = prefs2.iterator(); iterator
                    .hasNext();) {
                final Preference pref = iterator.next();
                if (relevantItemIDs.contains(pref.getItemID())) {
                    iterator.remove();
                }
            }
            if (!prefs2.isEmpty()) {
                trainingUsers.put(otherUserID, new GenericUserPreferenceArray(
                        prefs2));
            }
        } else {
            // otherwise just add all those other user's prefs
            trainingUsers.put(otherUserID, prefs2Array);
        }
    }
}
