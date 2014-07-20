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
import java.util.Iterator;
import java.util.List;

import org.codelibs.elasticsearch.taste.common.iterator.CountingIterator;

import com.google.common.base.Function;
import com.google.common.collect.Iterators;

/**
 * <p>
 * Like {@link BooleanUserPreferenceArray} but stores preferences for one item (all item IDs the same) rather
 * than one user.
 * </p>
 *
 * @see BooleanPreference
 * @see BooleanUserPreferenceArray
 * @see GenericItemPreferenceArray
 */
public final class BooleanItemPreferenceArray implements PreferenceArray {

    /**
     *
     */
    private static final long serialVersionUID = 1L;

    private final long[] ids;

    private long id;

    public BooleanItemPreferenceArray(final int size) {
        ids = new long[size];
        id = Long.MIN_VALUE; // as a sort of 'unspecified' value
    }

    public BooleanItemPreferenceArray(final List<? extends Preference> prefs,
            final boolean forOneUser) {
        this(prefs.size());
        final int size = prefs.size();
        for (int i = 0; i < size; i++) {
            final Preference pref = prefs.get(i);
            ids[i] = forOneUser ? pref.getItemID() : pref.getUserID();
        }
        if (size > 0) {
            id = forOneUser ? prefs.get(0).getUserID() : prefs.get(0)
                    .getItemID();
        }
    }

    /**
     * This is a private copy constructor for clone().
     */
    private BooleanItemPreferenceArray(final long[] ids, final long id) {
        this.ids = ids;
        this.id = id;
    }

    @Override
    public int length() {
        return ids.length;
    }

    @Override
    public Preference get(final int i) {
        return new PreferenceView(i);
    }

    @Override
    public void set(final int i, final Preference pref) {
        id = pref.getItemID();
        ids[i] = pref.getUserID();
    }

    @Override
    public long getUserID(final int i) {
        return ids[i];
    }

    @Override
    public void setUserID(final int i, final long userID) {
        ids[i] = userID;
    }

    @Override
    public long getItemID(final int i) {
        return id;
    }

    /**
     * {@inheritDoc}
     *
     * Note that this method will actually set the item ID for <em>all</em> preferences.
     */
    @Override
    public void setItemID(final int i, final long itemID) {
        id = itemID;
    }

    /**
     * @return all user IDs
     */
    @Override
    public long[] getIDs() {
        return ids;
    }

    @Override
    public float getValue(final int i) {
        return 1.0f;
    }

    @Override
    public void setValue(final int i, final float value) {
        throw new UnsupportedOperationException();
    }

    @Override
    public void sortByUser() {
        Arrays.sort(ids);
    }

    @Override
    public void sortByItem() {
    }

    @Override
    public void sortByValue() {
    }

    @Override
    public void sortByValueReversed() {
    }

    @Override
    public boolean hasPrefWithUserID(final long userID) {
        for (final long id : ids) {
            if (userID == id) {
                return true;
            }
        }
        return false;
    }

    @Override
    public boolean hasPrefWithItemID(final long itemID) {
        return id == itemID;
    }

    @Override
    public BooleanItemPreferenceArray clone() {
        return new BooleanItemPreferenceArray(ids.clone(), id);
    }

    @Override
    public int hashCode() {
        return (int) (id >> 32) ^ (int) id ^ Arrays.hashCode(ids);
    }

    @Override
    public boolean equals(final Object other) {
        if (!(other instanceof BooleanItemPreferenceArray)) {
            return false;
        }
        final BooleanItemPreferenceArray otherArray = (BooleanItemPreferenceArray) other;
        return id == otherArray.id && Arrays.equals(ids, otherArray.ids);
    }

    @Override
    public Iterator<Preference> iterator() {
        return Iterators.transform(new CountingIterator(length()),
                new Function<Integer, Preference>() {
                    @Override
                    public Preference apply(final Integer from) {
                        return new PreferenceView(from);
                    }
                });
    }

    @Override
    public String toString() {
        final StringBuilder result = new StringBuilder(10 * ids.length);
        result.append("BooleanItemPreferenceArray[itemID:");
        result.append(id);
        result.append(",{");
        for (int i = 0; i < ids.length; i++) {
            if (i > 0) {
                result.append(',');
            }
            result.append(ids[i]);
        }
        result.append("}]");
        return result.toString();
    }

    private final class PreferenceView implements Preference {

        private final int i;

        private PreferenceView(final int i) {
            this.i = i;
        }

        @Override
        public long getUserID() {
            return BooleanItemPreferenceArray.this.getUserID(i);
        }

        @Override
        public long getItemID() {
            return BooleanItemPreferenceArray.this.getItemID(i);
        }

        @Override
        public float getValue() {
            return 1.0f;
        }

        @Override
        public void setValue(final float value) {
            throw new UnsupportedOperationException();
        }

    }

}
