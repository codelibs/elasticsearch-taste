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

package org.codelibs.elasticsearch.taste.common;

import java.io.Serializable;
import java.util.Arrays;
import java.util.NoSuchElementException;

import org.apache.mahout.common.RandomUtils;

import com.google.common.base.Preconditions;

/**
 * @see FastByIDMap
 */
public final class FastIDSet implements Serializable, Cloneable, Iterable<Long> {

    /**
     *
     */
    private static final long serialVersionUID = 1L;

    private static final float DEFAULT_LOAD_FACTOR = 1.5f;

    /** Dummy object used to represent a key that has been removed. */
    private static final long REMOVED = Long.MAX_VALUE;

    private static final long NULL = Long.MIN_VALUE;

    private long[] keys;

    private float loadFactor;

    private int numEntries;

    private int numSlotsUsed;

    /** Creates a new {@link FastIDSet} with default capacity. */
    public FastIDSet() {
        this(2);
    }

    public FastIDSet(final long[] initialKeys) {
        this(initialKeys.length);
        addAll(initialKeys);
    }

    public FastIDSet(final int size) {
        this(size, DEFAULT_LOAD_FACTOR);
    }

    public FastIDSet(final int size, final float loadFactor) {
        Preconditions.checkArgument(size >= 0, "size must be at least 0");
        Preconditions.checkArgument(loadFactor >= 1.0f,
                "loadFactor must be at least 1.0");
        this.loadFactor = loadFactor;
        final int max = (int) (RandomUtils.MAX_INT_SMALLER_TWIN_PRIME / loadFactor);
        Preconditions.checkArgument(size < max, "size must be less than %d",
                max);
        final int hashSize = RandomUtils
                .nextTwinPrime((int) (loadFactor * size));
        keys = new long[hashSize];
        Arrays.fill(keys, NULL);
    }

    /**
     * @see #findForAdd(long)
     */
    private int find(final long key) {
        final int theHashCode = (int) key & 0x7FFFFFFF; // make sure it's positive
        final long[] keys = this.keys;
        final int hashSize = keys.length;
        final int jump = 1 + theHashCode % (hashSize - 2);
        int index = theHashCode % hashSize;
        long currentKey = keys[index];
        while (currentKey != NULL && key != currentKey) { // note: true when currentKey == REMOVED
            index -= index < jump ? jump - hashSize : jump;
            currentKey = keys[index];
        }
        return index;
    }

    /**
     * @see #find(long)
     */
    private int findForAdd(final long key) {
        final int theHashCode = (int) key & 0x7FFFFFFF; // make sure it's positive
        final long[] keys = this.keys;
        final int hashSize = keys.length;
        final int jump = 1 + theHashCode % (hashSize - 2);
        int index = theHashCode % hashSize;
        long currentKey = keys[index];
        while (currentKey != NULL && currentKey != REMOVED && key != currentKey) {
            index -= index < jump ? jump - hashSize : jump;
            currentKey = keys[index];
        }
        if (currentKey != REMOVED) {
            return index;
        }
        // If we're adding, it's here, but, the key might have a value already later
        final int addIndex = index;
        while (currentKey != NULL && key != currentKey) {
            index -= index < jump ? jump - hashSize : jump;
            currentKey = keys[index];
        }
        return key == currentKey ? index : addIndex;
    }

    public int size() {
        return numEntries;
    }

    public boolean isEmpty() {
        return numEntries == 0;
    }

    public boolean contains(final long key) {
        return key != NULL && key != REMOVED && keys[find(key)] != NULL;
    }

    public boolean add(final long key) {
        Preconditions.checkArgument(key != NULL && key != REMOVED);

        // If less than half the slots are open, let's clear it up
        if (numSlotsUsed * loadFactor >= keys.length) {
            // If over half the slots used are actual entries, let's grow
            if (numEntries * loadFactor >= numSlotsUsed) {
                growAndRehash();
            } else {
                // Otherwise just rehash to clear REMOVED entries and don't grow
                rehash();
            }
        }
        // Here we may later consider implementing Brent's variation described on page 532
        final int index = findForAdd(key);
        final long keyIndex = keys[index];
        if (keyIndex != key) {
            keys[index] = key;
            numEntries++;
            if (keyIndex == NULL) {
                numSlotsUsed++;
            }
            return true;
        }
        return false;
    }

    @Override
    public LongPrimitiveIterator iterator() {
        return new KeyIterator();
    }

    public long[] toArray() {
        final long[] result = new long[numEntries];
        for (int i = 0, position = 0; i < result.length; i++) {
            while (keys[position] == NULL || keys[position] == REMOVED) {
                position++;
            }
            result[i] = keys[position++];
        }
        return result;
    }

    public boolean remove(final long key) {
        if (key == NULL || key == REMOVED) {
            return false;
        }
        final int index = find(key);
        if (keys[index] == NULL) {
            return false;
        } else {
            keys[index] = REMOVED;
            numEntries--;
            return true;
        }
    }

    public boolean addAll(final long[] c) {
        boolean changed = false;
        for (final long k : c) {
            if (add(k)) {
                changed = true;
            }
        }
        return changed;
    }

    public boolean addAll(final FastIDSet c) {
        boolean changed = false;
        for (final long k : c.keys) {
            if (k != NULL && k != REMOVED && add(k)) {
                changed = true;
            }
        }
        return changed;
    }

    public boolean removeAll(final long[] c) {
        boolean changed = false;
        for (final long o : c) {
            if (remove(o)) {
                changed = true;
            }
        }
        return changed;
    }

    public boolean removeAll(final FastIDSet c) {
        boolean changed = false;
        for (final long k : c.keys) {
            if (k != NULL && k != REMOVED && remove(k)) {
                changed = true;
            }
        }
        return changed;
    }

    public boolean retainAll(final FastIDSet c) {
        boolean changed = false;
        for (int i = 0; i < keys.length; i++) {
            final long k = keys[i];
            if (k != NULL && k != REMOVED && !c.contains(k)) {
                keys[i] = REMOVED;
                numEntries--;
                changed = true;
            }
        }
        return changed;
    }

    public void clear() {
        numEntries = 0;
        numSlotsUsed = 0;
        Arrays.fill(keys, NULL);
    }

    private void growAndRehash() {
        if (keys.length * loadFactor >= RandomUtils.MAX_INT_SMALLER_TWIN_PRIME) {
            throw new IllegalStateException("Can't grow any more");
        }
        rehash(RandomUtils.nextTwinPrime((int) (loadFactor * keys.length)));
    }

    public void rehash() {
        rehash(RandomUtils.nextTwinPrime((int) (loadFactor * numEntries)));
    }

    private void rehash(final int newHashSize) {
        final long[] oldKeys = keys;
        numEntries = 0;
        numSlotsUsed = 0;
        keys = new long[newHashSize];
        Arrays.fill(keys, NULL);
        for (final long key : oldKeys) {
            if (key != NULL && key != REMOVED) {
                add(key);
            }
        }
    }

    /**
     * Convenience method to quickly compute just the size of the intersection with another {@link FastIDSet}.
     *
     * @param other
     *          {@link FastIDSet} to intersect with
     * @return number of elements in intersection
     */
    public int intersectionSize(final FastIDSet other) {
        int count = 0;
        for (final long key : other.keys) {
            if (key != NULL && key != REMOVED && keys[find(key)] != NULL) {
                count++;
            }
        }
        return count;
    }

    @Override
    public FastIDSet clone() {
        FastIDSet clone;
        try {
            clone = (FastIDSet) super.clone();
        } catch (final CloneNotSupportedException cnse) {
            throw new AssertionError();
        }
        clone.keys = keys.clone();
        return clone;
    }

    @Override
    public int hashCode() {
        int hash = 0;
        final long[] keys = this.keys;
        for (final long key : keys) {
            if (key != NULL && key != REMOVED) {
                hash = 31 * hash + ((int) (key >> 32) ^ (int) key);
            }
        }
        return hash;
    }

    @Override
    public boolean equals(final Object other) {
        if (!(other instanceof FastIDSet)) {
            return false;
        }
        final FastIDSet otherMap = (FastIDSet) other;
        final long[] otherKeys = otherMap.keys;
        final int length = keys.length;
        final int otherLength = otherKeys.length;
        final int max = Math.min(length, otherLength);

        int i = 0;
        while (i < max) {
            final long key = keys[i];
            final long otherKey = otherKeys[i];
            if (key == NULL || key == REMOVED) {
                if (otherKey != NULL && otherKey != REMOVED) {
                    return false;
                }
            } else {
                if (key != otherKey) {
                    return false;
                }
            }
            i++;
        }
        while (i < length) {
            final long key = keys[i];
            if (key != NULL && key != REMOVED) {
                return false;
            }
            i++;
        }
        while (i < otherLength) {
            final long key = otherKeys[i];
            if (key != NULL && key != REMOVED) {
                return false;
            }
            i++;
        }
        return true;
    }

    @Override
    public String toString() {
        if (isEmpty()) {
            return "[]";
        }
        final StringBuilder result = new StringBuilder();
        result.append('[');
        for (final long key : keys) {
            if (key != NULL && key != REMOVED) {
                result.append(key).append(',');
            }
        }
        result.setCharAt(result.length() - 1, ']');
        return result.toString();
    }

    private final class KeyIterator extends AbstractLongPrimitiveIterator {

        private int position;

        private int lastNext = -1;

        @Override
        public boolean hasNext() {
            goToNext();
            return position < keys.length;
        }

        @Override
        public long nextLong() {
            goToNext();
            lastNext = position;
            if (position >= keys.length) {
                throw new NoSuchElementException();
            }
            return keys[position++];
        }

        @Override
        public long peek() {
            goToNext();
            if (position >= keys.length) {
                throw new NoSuchElementException();
            }
            return keys[position];
        }

        private void goToNext() {
            final int length = keys.length;
            while (position < length
                    && (keys[position] == NULL || keys[position] == REMOVED)) {
                position++;
            }
        }

        @Override
        public void remove() {
            if (lastNext >= keys.length) {
                throw new NoSuchElementException();
            }
            if (lastNext < 0) {
                throw new IllegalStateException();
            }
            keys[lastNext] = REMOVED;
            numEntries--;
        }

        @Override
        public void skip(final int n) {
            position += n;
        }

    }

}
