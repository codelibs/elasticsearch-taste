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

import com.google.common.collect.Iterators;

/**
 * <p>
 * Like {@link GenericUserPreferenceArray} but stores, conceptually, {@link BooleanPreference} objects which
 * have no associated preference value.
 * </p>
 *
 * @see BooleanPreference
 * @see BooleanItemPreferenceArray
 * @see GenericUserPreferenceArray
 */
public final class BooleanUserPreferenceArray implements PreferenceArray {

    /**
     *
     */
    private static final long serialVersionUID = 1L;

    private final long[] ids;

    private long id;

    public BooleanUserPreferenceArray(final int size) {
        ids = new long[size];
        id = Long.MIN_VALUE; // as a sort of 'unspecified' value
    }

    public BooleanUserPreferenceArray(final List<? extends Preference> prefs) {
        this(prefs.size());
        final int size = prefs.size();
        for (int i = 0; i < size; i++) {
            final Preference pref = prefs.get(i);
            ids[i] = pref.getItemID();
        }
        if (size > 0) {
            id = prefs.get(0).getUserID();
        }
    }

    /**
     * This is a private copy constructor for clone().
     */
    private BooleanUserPreferenceArray(final long[] ids, final long id) {
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
        id = pref.getUserID();
        ids[i] = pref.getItemID();
    }

    @Override
    public long getUserID(final int i) {
        return id;
    }

    /**
     * {@inheritDoc}
     *
     * Note that this method will actually set the user ID for <em>all</em> preferences.
     */
    @Override
    public void setUserID(final int i, final long userID) {
        id = userID;
    }

    @Override
    public long getItemID(final int i) {
        return ids[i];
    }

    @Override
    public void setItemID(final int i, final long itemID) {
        ids[i] = itemID;
    }

    /**
     * @return all item IDs
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
    }

    @Override
    public void sortByItem() {
        Arrays.sort(ids);
    }

    @Override
    public void sortByValue() {
    }

    @Override
    public void sortByValueReversed() {
    }

    @Override
    public boolean hasPrefWithUserID(final long userID) {
        return id == userID;
    }

    @Override
    public boolean hasPrefWithItemID(final long itemID) {
        for (final long id : ids) {
            if (itemID == id) {
                return true;
            }
        }
        return false;
    }

    @Override
    public BooleanUserPreferenceArray clone() {
        return new BooleanUserPreferenceArray(ids.clone(), id);
    }

    @Override
    public int hashCode() {
        return (int) (id >> 32) ^ (int) id ^ Arrays.hashCode(ids);
    }

    @Override
    public boolean equals(final Object other) {
        if (!(other instanceof BooleanUserPreferenceArray)) {
            return false;
        }
        final BooleanUserPreferenceArray otherArray = (BooleanUserPreferenceArray) other;
        return id == otherArray.id && Arrays.equals(ids, otherArray.ids);
    }

    @Override
    public Iterator<Preference> iterator() {
        return Iterators.transform(new CountingIterator(length()),
                from -> new PreferenceView(from));
    }

    @Override
    public String toString() {
        final StringBuilder result = new StringBuilder(10 * ids.length);
        result.append("BooleanUserPreferenceArray[userID:");
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
            return BooleanUserPreferenceArray.this.getUserID(i);
        }

        @Override
        public long getItemID() {
            return BooleanUserPreferenceArray.this.getItemID(i);
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
