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
 * Like {@link GenericUserPreferenceArray} but stores preferences for one item (all item IDs the same) rather
 * than one user.
 * </p>
 *
 * @see BooleanItemPreferenceArray
 * @see GenericUserPreferenceArray
 * @see GenericPreference
 */
public final class GenericItemPreferenceArray implements PreferenceArray {

    /**
     *
     */
    private static final long serialVersionUID = 1L;

    private static final int USER = 0;

    private static final int VALUE = 2;

    private static final int VALUE_REVERSED = 3;

    private final long[] ids;

    private long id;

    private final float[] values;

    public GenericItemPreferenceArray(final int size) {
        ids = new long[size];
        values = new float[size];
        id = Long.MIN_VALUE; // as a sort of 'unspecified' value
    }

    public GenericItemPreferenceArray(final List<? extends Preference> prefs) {
        this(prefs.size());
        final int size = prefs.size();
        long itemID = Long.MIN_VALUE;
        for (int i = 0; i < size; i++) {
            final Preference pref = prefs.get(i);
            ids[i] = pref.getUserID();
            if (i == 0) {
                itemID = pref.getItemID();
            } else {
                if (itemID != pref.getItemID()) {
                    throw new IllegalArgumentException(
                            "Not all item IDs are the same");
                }
            }
            values[i] = pref.getValue();
        }
        id = itemID;
    }

    /**
     * This is a private copy constructor for clone().
     */
    private GenericItemPreferenceArray(final long[] ids, final long id,
            final float[] values) {
        this.ids = ids;
        this.id = id;
        this.values = values;
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
        values[i] = pref.getValue();
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
        return values[i];
    }

    @Override
    public void setValue(final int i, final float value) {
        values[i] = value;
    }

    @Override
    public void sortByUser() {
        lateralSort(USER);
    }

    @Override
    public void sortByItem() {
    }

    @Override
    public void sortByValue() {
        lateralSort(VALUE);
    }

    @Override
    public void sortByValueReversed() {
        lateralSort(VALUE_REVERSED);
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

    private void lateralSort(final int type) {
        //Comb sort: http://en.wikipedia.org/wiki/Comb_sort
        final int length = length();
        int gap = length;
        boolean swapped = false;
        while (gap > 1 || swapped) {
            if (gap > 1) {
                gap /= 1.247330950103979; // = 1 / (1 - 1/e^phi)
            }
            swapped = false;
            final int max = length - gap;
            for (int i = 0; i < max; i++) {
                final int other = i + gap;
                if (isLess(other, i, type)) {
                    swap(i, other);
                    swapped = true;
                }
            }
        }
    }

    private boolean isLess(final int i, final int j, final int type) {
        switch (type) {
        case USER:
            return ids[i] < ids[j];
        case VALUE:
            return values[i] < values[j];
        case VALUE_REVERSED:
            return values[i] > values[j];
        default:
            throw new IllegalStateException();
        }
    }

    private void swap(final int i, final int j) {
        final long temp1 = ids[i];
        final float temp2 = values[i];
        ids[i] = ids[j];
        values[i] = values[j];
        ids[j] = temp1;
        values[j] = temp2;
    }

    @Override
    public GenericItemPreferenceArray clone() {
        return new GenericItemPreferenceArray(ids.clone(), id, values.clone());
    }

    @Override
    public int hashCode() {
        return (int) (id >> 32) ^ (int) id ^ Arrays.hashCode(ids)
                ^ Arrays.hashCode(values);
    }

    @Override
    public boolean equals(final Object other) {
        if (!(other instanceof GenericItemPreferenceArray)) {
            return false;
        }
        final GenericItemPreferenceArray otherArray = (GenericItemPreferenceArray) other;
        return id == otherArray.id && Arrays.equals(ids, otherArray.ids)
                && Arrays.equals(values, otherArray.values);
    }

    @Override
    public Iterator<Preference> iterator() {
        return Iterators.transform(new CountingIterator(length()),
                from -> new PreferenceView(from));
    }

    @Override
    public String toString() {
        if (ids == null || ids.length == 0) {
            return "GenericItemPreferenceArray[{}]";
        }
        final StringBuilder result = new StringBuilder(20 * ids.length);
        result.append("GenericItemPreferenceArray[itemID:");
        result.append(id);
        result.append(",{");
        for (int i = 0; i < ids.length; i++) {
            if (i > 0) {
                result.append(',');
            }
            result.append(ids[i]);
            result.append('=');
            result.append(values[i]);
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
            return GenericItemPreferenceArray.this.getUserID(i);
        }

        @Override
        public long getItemID() {
            return GenericItemPreferenceArray.this.getItemID(i);
        }

        @Override
        public float getValue() {
            return values[i];
        }

        @Override
        public void setValue(final float value) {
            values[i] = value;
        }

    }

}
